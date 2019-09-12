use chrono::prelude::*;
use crate::prelude::*;
use log::{debug};
use lz4::{Decoder, EncoderBuilder};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::{self, Seek, SeekFrom, Read, Write};
use std::marker::PhantomData;
use std::mem;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::fmt::Debug;

pub const INDEX_ENTRY_SIZE: usize = mem::size_of::<IndexEntry>();

/// Phantom type for Bucket<T>; state when the Bucket is opened, but headers
/// haven't yet been checked.
pub struct Initial;

/// Phantom type for Bucket<T>; state when the Bucket is opened and the
/// headers have been checked and validated.
pub struct Checked;

/// A bucket is backed by a file on disk; the file descriptor is
/// wrapped in a buffered reader to reduce the number of syscalls when
/// querying the database.
pub struct Bucket<T> {
    phantom: PhantomData<T>,
    pub header: BucketHeader,
    pub file: BufReader<File>,
    pub path: PathBuf,
}

/// The headers of a database; they are used to determine if a
/// database file can be opened by binstore.
#[derive(Debug, Deserialize, Serialize)]
pub struct BucketHeader {
    pub magic: u32,
    pub version: u32,
    pub timestamp: i64,
    pub si_base_offset: u64,
    pub di_base_offset: u64,
    pub data_base_offset: u64,
    pub num_entries: u64,
}

/// A small index that can be quickly loaded in memory.
#[derive(Debug, Deserialize, Serialize)]
pub struct SparseIndex {
    step: usize,
    index: Vec<IndexEntry>,
}

/// An entry in the full index; the offset points into the data
/// section where the set of Values is stored.
#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct IndexEntry {
    pub key: u64,
    pub offset: u64,
}

impl Default for BucketHeader {
    fn default() -> BucketHeader {
        BucketHeader {
            magic: MAGIC,
            version: VERSION,
            timestamp: Local::now().timestamp(),
            si_base_offset: 0,
            di_base_offset: 0,
            data_base_offset: 0,
            num_entries: 0,
        }
    }
}

impl Default for SparseIndex {
    fn default() -> SparseIndex {
        SparseIndex {
            index: Vec::with_capacity(1024),
            step: DEFAULT_SPARSE_INDEX_STEP,
        }
    }
}

impl Bucket<Initial> {
    pub fn open<P: AsRef<Path>>(filename: P) -> Result<Bucket<Initial>> {
        let file = File::open(filename.as_ref())?;
        let reader = BufReader::new(file);
        let path = PathBuf::from(filename.as_ref());
        let bucket = Bucket { phantom: PhantomData, file: reader, header: BucketHeader::default(), path };
        Ok(bucket)
    }

    pub fn check_headers(mut self) -> Result<Bucket<Checked>> {
        let header: BucketHeader = bincode::deserialize_from(&mut self.file)?;
        if header.magic != MAGIC {
            return Err(Error::BadMagic);
        }
        if header.version != VERSION {
            return Err(Error::BadVersion);
        }
        let bucket = Bucket { phantom: PhantomData, file: self.file, header: header, path: self.path };
        Ok(bucket)
    }
}

impl SparseIndex {
    pub fn try_get(&self, key: u64) -> Option<(u64, u64)> {
        if self.index.len() < 2 {
            return None;
        }
        match self.index.binary_search_by_key(&key, |&entry| entry.key) {
            Ok(i) => Some((self.index[i].offset, self.index[i].offset)),
            Err(0) => None,
            Err(closest) => {
                if closest == self.index.len() {
                    return None;
                }
                Some((self.index[closest - 1].offset, self.index[closest].offset))
            }
        }
    }

    /// Creates a new SparseIndex.
    /// If `entries` is empty, we return an empty SparseIndex.
    /// If `entries` is not empty, we return a SparseIndex
    /// where the first pair in the index is the smallest key
    /// of `entries` and the last pair is the largest key of
    /// `entries`.

    pub fn new(entries: &BTreeSet<HashedKey>) -> Self {
        return Self::new_with_step(DEFAULT_SPARSE_INDEX_STEP, entries);
    }

    pub fn new_with_step(step: usize, entries: &BTreeSet<HashedKey>) -> Self {
        let mut si = SparseIndex::default();
        si.step = step;

        // If there is no max entry, that means `entries` is empty
        // and we should return an empty sparse index.
        let last_key = match entries.iter().max() {
            Some(key) => *key,
            None => return si,
        };

        for (i, key) in entries.iter().enumerate().step_by(si.step) {
            let di_offset = i * INDEX_ENTRY_SIZE;
            si.index.push(IndexEntry {
                key: *key,
                offset: di_offset as u64,
            });
        }

        let needs_one_extra =
            entries.len() == 1
            || (si.index[si.index.len() - 1].key != last_key);

        if needs_one_extra {
            let di_offset = (entries.len() - 1) * INDEX_ENTRY_SIZE;
            si.index.push(IndexEntry {
                key: last_key,
                offset: di_offset as u64,
            });
        }

        return si;
    }

    pub fn size(&self) -> u64 {
        bincode::serialized_size(&self).expect("SparseIndex::size()") as u64
    }
}


impl Bucket<Checked> {
    pub fn read_sparse_index(&mut self) -> Result<SparseIndex> {
        let si: SparseIndex = bincode::deserialize_from(&mut self.file)?;
        return Ok(si);
    }

    fn locate(&mut self, key: HashedKey, di_off1: u64, di_off2: u64) -> Result<Option<u64>> {
        let mut curr_offset = di_off1 + self.header.di_base_offset;
        let last_offset = di_off2 + self.header.di_base_offset;
        self.file.seek(SeekFrom::Start(curr_offset))?;
        loop {
            let IndexEntry {
                key: k,
                offset: off,
            } = bincode::deserialize_from(&mut self.file)?;
            if k == key {
                return Ok(Some(off + self.header.data_base_offset));
            }
            curr_offset += INDEX_ENTRY_SIZE as u64;
            if curr_offset > last_offset {
                break;
            }
        }
        return Ok(None);
    }

    pub fn try_get(&mut self, key: HashedKey, di_off1: u64, di_off2: u64) -> Result<Option<BTreeSet<Value>>> {
        let t = Instant::now();
        let off_option = self.locate(key, di_off1, di_off2)?;
        debug!("dense index search time: {:?}", t.elapsed());

        match off_option {
            Some(offset) => {
                let t = Instant::now();
                self.file.seek(SeekFrom::Start(offset))?;
                let values = read_values(&mut self.file)?;
                debug!("read_values: {:?}", t.elapsed());
                return Ok(Some(values));
            }
            None => {
                return Ok(None);
            }
        }
    }

    pub fn get(&mut self, hash: HashedKey) -> Result<Option<BTreeSet<Value>>> {
        let si = self.read_sparse_index()?;
        let (offset_1, offset_2) =
            match si.try_get(hash) {
                Some((off_1, off_2)) => (off_1, off_2),
                None => {
                    return Ok(None);
                }
            };
        self.try_get(hash, offset_1, offset_2)
    }
}

fn write_values<W: Write>(w: &mut W, values: &BTreeSet<Value>) -> Result<()> {
    let values_bin: Vec<u8> = bincode::serialize(&values)?;
    let mut refu8: &[u8] = values_bin.as_ref();
    let mut encoder = EncoderBuilder::new()
        .level(COMPRESSION_LEVEL)
        .build(w)?;
    io::copy(&mut refu8, &mut encoder)?;
    encoder.finish();
    return Ok(());
}

fn read_values<R: Read>(r: &mut R) -> Result<BTreeSet<Value>> {
    let mut bincode: Vec<u8> = Vec::new();
    let mut lz4_decoder = Decoder::new(r)?;
    io::copy(&mut lz4_decoder, &mut bincode)?;
    let u8_ref: &[u8] = bincode.as_ref();
    let values: BTreeSet<Value> = bincode::deserialize_from(u8_ref)?;
    return Ok(values);
}

pub fn delete<P: AsRef<Path> + Debug>(path: P, new_bucket: P, value_set: &[Value]) -> Result<()> {
    let t = Instant::now();
    // Open the database twice: once to have a cursor in the dense
    // index; once to have a cursor in the data section.
    let mut bucket = Bucket::open(&path)?.check_headers()?;
    let mut bucket_data = Bucket::open(&path)?.check_headers()?;
    debug!("opened {:?} in {:?}", path.as_ref(), t.elapsed());

    // The BTreeMap that will be used to create a new binstore file.
    let mut bmap: BTreeMap<HashedKey, BTreeSet<Value>> = BTreeMap::new();

    // Position the cursors.
    bucket.file.seek(SeekFrom::Start(bucket.header.di_base_offset))?;
    bucket_data.file.seek(SeekFrom::Start(bucket_data.header.data_base_offset))?;

    for _ in 0..bucket.header.num_entries {
        let IndexEntry {
            key: k,
            offset: off,
        } = bincode::deserialize_from(&mut bucket.file)?;
        bucket_data.file.seek(SeekFrom::Start(bucket_data.header.data_base_offset + off))?;
        let mut values = read_values(&mut bucket_data.file)?;
        for t in value_set {
            values.remove(t);
        }
        if !values.is_empty() {
            bmap.insert(k, values);
        }
    }

    create(new_bucket, &bmap)?;

    Ok(())
}

pub fn create<P: AsRef<Path>>(filename: P, entries: &BTreeMap<u64, BTreeSet<Value>>) -> Result<()> {
    let file = File::create(filename.as_ref())?;
    let mut w = BufWriter::new(file);

    // Write default headers to reserve space in file.
    let mut header = BucketHeader::default();
    bincode::serialize_into(&mut w, &header)?;

    header.num_entries = entries.len() as u64;

    // Build the sparse index
    header.si_base_offset = tell(&mut w)?;

    let b: BTreeSet<HashedKey> = entries.iter().map(|(key, _)| *key).collect();

    let si = SparseIndex::new(&b);
    bincode::serialize_into(&mut w, &si)?;

    // Figure out the size of the dense index and seek ahead, leaving
    // zeros behind.  After we've written the data section, we'll come
    // back to backpatch this section.
    //
    header.di_base_offset = tell(&mut w)?;
    let di_size = (entries.len() * INDEX_ENTRY_SIZE) as u64;
    header.data_base_offset = w.seek(SeekFrom::Current(di_size as i64))?;

    // Populate the data section.
    let mut curr_offset: u64 = 0;
    let mut offsets: Vec<u64> = Vec::with_capacity(entries.len());
    for (_, values) in entries.iter() {
        offsets.push(curr_offset);
        write_values(&mut w, values)?;
        curr_offset = tell(&mut w)? - header.data_base_offset;
    }

    // Go back to the dense index and insert the data section offsets.
    w.seek(SeekFrom::Start(header.di_base_offset))?;
    for ((key, _), offset) in entries.iter().zip(offsets.iter()) {
        let entry = IndexEntry {
            key: *key,
            offset: *offset,
        };
        bincode::serialize_into(&mut w, &entry)?;
    }

    // Rewrite header
    w.seek(SeekFrom::Start(0))?;
    bincode::serialize_into(&mut w, &header)?;

    Ok(())
}

/// Merges two binstore files, and returns a BTreeMap that can be fed to
/// Bucket::create (or an error).
pub fn merge<P: AsRef<Path>>(filename1: P, filename2: P, output_file: P) -> Result<()> {
    enum Origin {
        Bucket1 { offset: u64 },
        Bucket2 { offset: u64 },
        Union { offset_1: u64, offset_2: u64 },
    };
    struct Source {
        key: HashedKey,
        origin: Origin
    };

    fn merge_into<W: Write + Seek>(source: Source,
                  bucket_1_data: &mut Bucket<Checked>,
                  bucket_2_data: &mut Bucket<Checked>,
                  output_di: &mut W,
                  output_data: &mut W,
                  data_base_offset: u64)
                  -> Result<()>
    {
        let offset = tell(output_data)?;
        let relative_offset = offset - data_base_offset;
        let di_entry = IndexEntry { key: source.key, offset: relative_offset };
        bincode::serialize_into(output_di, &di_entry)?;
        match source.origin {
            Origin::Bucket1 { offset } => {
                bucket_1_data.file.seek(SeekFrom::Start(bucket_1_data.header.data_base_offset + offset))?;
                let values = read_values(&mut bucket_1_data.file)?;
                write_values(output_data, &values)?;
            },
            Origin::Bucket2 { offset } => {
                bucket_2_data.file.seek(SeekFrom::Start(bucket_2_data.header.data_base_offset + offset))?;
                let values = read_values(&mut bucket_2_data.file)?;
                write_values(output_data, &values)?;
            },
            Origin::Union { offset_1, offset_2 } => {
                bucket_1_data.file.seek(SeekFrom::Start(bucket_1_data.header.data_base_offset + offset_1))?;
                let mut values_1 = read_values(&mut bucket_1_data.file)?;
                bucket_2_data.file.seek(SeekFrom::Start(bucket_2_data.header.data_base_offset + offset_2))?;
                let values_2 = read_values(&mut bucket_2_data.file)?;
                for value in values_2 {
                    values_1.insert(value);
                }
                write_values(output_data, &values_1)?;
            }
        }
        return Ok(());
    }

    fn accumulate_keys_in_bset(bucket_1: &mut Bucket<Checked>,
                  bucket_2: &mut Bucket<Checked>)
                   -> Result<BTreeSet<HashedKey>>
    {
        // The offsets in the dense indexes
        let mut curr_offset_1 = bucket_1.header.di_base_offset;
        let mut curr_offset_2 = bucket_2.header.di_base_offset;

        // Where the dense indexes stop.
        let data_start_1 = bucket_1.header.data_base_offset;
        let data_start_2 = bucket_2.header.data_base_offset;

        // Position the cursors.
        bucket_1.file.seek(SeekFrom::Start(curr_offset_1))?;
        bucket_2.file.seek(SeekFrom::Start(curr_offset_2))?;

        let mut bset = BTreeSet::new();

        while curr_offset_1 < data_start_1 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_1.file)?;
            bset.insert(entry.key);
            curr_offset_1 += INDEX_ENTRY_SIZE as u64;
        }

        while curr_offset_2 < data_start_2 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_2.file)?;
            bset.insert(entry.key);
            curr_offset_2 += INDEX_ENTRY_SIZE as u64;
        }

        // Restore the cursor positions
        bucket_1.file.seek(SeekFrom::Start(bucket_1.header.di_base_offset))?;
        bucket_2.file.seek(SeekFrom::Start(bucket_2.header.di_base_offset))?;

        Ok(bset)
    }

    // Open the database twice: once to have a cursor in the dense
    // index; once to have a cursor in the data section.
    let mut bucket_1 = Bucket::open(filename1.as_ref())?.check_headers()?;
    let mut bucket_2 = Bucket::open(filename2.as_ref())?.check_headers()?;
    let mut data_1 = Bucket::open(filename1.as_ref())?.check_headers()?;
    let mut data_2 = Bucket::open(filename2.as_ref())?.check_headers()?;

    // Where the dense indexes stop.
    let data_start_1 = bucket_1.header.data_base_offset;
    let data_start_2 = bucket_2.header.data_base_offset;

    // The last key read from bucket_1 and bucket_2.
    let mut ci_1 = 0;
    let mut data_off_1 = 0;
    let mut ci_2 = 0;
    let mut data_off_2 = 0;

    // If true, a read in the database dense index must be performed
    // to obtain a new key.
    let mut read_bucket_1 = true;
    let mut read_bucket_2 = true;

    // The offsets in the dense indexes (this is an optimization to
    // avoid using tell() all the time).
    let mut curr_offset_1 = bucket_1.header.di_base_offset;
    let mut curr_offset_2 = bucket_2.header.di_base_offset;

    // Position the cursors.
    data_1.file.seek(SeekFrom::Start(data_1.header.data_base_offset))?;
    data_2.file.seek(SeekFrom::Start(data_2.header.data_base_offset))?;

    // Set up the output bucket.
    let file = File::create(output_file.as_ref())?;
    let mut output = BufWriter::new(file);

    let file = File::create(output_file.as_ref())?;
    let mut output_data = BufWriter::new(file);

    // Write default headers to reserve space in file.
    let mut header = BucketHeader::default();
    bincode::serialize_into(&mut output, &header)?;

    // Build the sparse index.
    header.si_base_offset = tell(&mut output)?;
    let b = accumulate_keys_in_bset(&mut bucket_1, &mut bucket_2)?;
    let si = SparseIndex::new(&b);
    bincode::serialize_into(&mut output, &si)?;

    header.num_entries = b.len() as u64;

    // Figure out the size of the dense index and place the cursor of
    // `output_data` at the end of it. Leave the cursor for `output` to point
    // to the begining of the dense index.
    header.di_base_offset = tell(&mut output)?;
    let di_size = (b.len() * INDEX_ENTRY_SIZE) as u64;
    header.data_base_offset = output_data.seek(SeekFrom::Current(header.di_base_offset as i64 + di_size as i64))?;

    // Populate the data section.
    while curr_offset_1 < data_start_1 && curr_offset_2 < data_start_2 {
        if read_bucket_1 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_1.file)?;
            ci_1 = entry.key;
            data_off_1 = entry.offset;
        }

        if read_bucket_2 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_2.file)?;
            ci_2 = entry.key;
            data_off_2 = entry.offset;
        }

        if ci_1 < ci_2 {
            merge_into(Source {
                    key: ci_1,
                    origin: Origin::Bucket1 { offset: data_off_1 }
                },
                &mut data_1,
                &mut data_2,
                &mut output,
                &mut output_data,
                header.data_base_offset)?;
            curr_offset_1 += INDEX_ENTRY_SIZE as u64;
            read_bucket_1 = true;
            read_bucket_2 = false;
        } else if ci_1 > ci_2 {
            merge_into(Source {
                    key: ci_2,
                    origin: Origin::Bucket2 { offset: data_off_2 }
                },
                &mut data_1,
                &mut data_2,
                &mut output,
                &mut output_data,
                header.data_base_offset)?;
            curr_offset_2 += INDEX_ENTRY_SIZE as u64;
            read_bucket_1 = false;
            read_bucket_2 = true;
        } else {
            merge_into(Source {
                    key: ci_1,
                    origin: Origin::Union { offset_1: data_off_1, offset_2: data_off_2 }
                },
                &mut data_1,
                &mut data_2,
                &mut output,
                &mut output_data,
                header.data_base_offset)?;
            curr_offset_1 += INDEX_ENTRY_SIZE as u64;
            curr_offset_2 += INDEX_ENTRY_SIZE as u64;
            read_bucket_1 = true;
            read_bucket_2 = true;
        }
    }

    while curr_offset_1 < data_start_1 {
        if read_bucket_1 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_1.file)?;
            ci_1 = entry.key;
            data_off_1 = entry.offset;
        }
        merge_into(Source {
                key: ci_1,
                origin: Origin::Bucket1 { offset: data_off_1 }
            },
            &mut data_1,
            &mut data_2,
            &mut output,
            &mut output_data,
            header.data_base_offset)?;
        curr_offset_1 += INDEX_ENTRY_SIZE as u64;
        read_bucket_1 = true;
    }

    while curr_offset_2 < data_start_2 {
        if read_bucket_2 {
            let entry: IndexEntry = bincode::deserialize_from(&mut bucket_2.file)?;
            ci_2 = entry.key;
            data_off_2 = entry.offset;
        };
        merge_into(Source {
                key: ci_2,
                origin: Origin::Bucket2 { offset: data_off_2 }
            },
            &mut data_1,
            &mut data_2,
            &mut output,
            &mut output_data,
            header.data_base_offset)?;
        curr_offset_2 += INDEX_ENTRY_SIZE as u64;
        read_bucket_2 = true;
    }

    // Rewrite header
    output.seek(SeekFrom::Start(0))?;
    bincode::serialize_into(&mut output, &header)?;

    return Ok(());
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use crate::error::Error;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use std::collections::BTreeSet;
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_header() {
        // Correct magic and version
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let header = BucketHeader::default();
            bincode::serialize_into(&mut tmp, &header).expect("bincode");
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            assert!(bucket.check_headers().is_ok());
        }

        // Incorrect magic
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let header = BucketHeader { magic: MAGIC+1, ..BucketHeader::default() };
            bincode::serialize_into(&mut tmp, &header).expect("bincode");
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            assert!(match bucket.check_headers() {
                Err(Error::BadMagic) => true,
                _ => false,
            });
        }

        // Incorrect version
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let header = BucketHeader { version: VERSION+1, ..BucketHeader::default() };
            bincode::serialize_into(&mut tmp, &header).expect("bincode");
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            assert!(match bucket.check_headers() {
                Err(Error::BadVersion) => true,
                _ => false,
            });
        }


        // Incorrect magic and version
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let header = BucketHeader { magic: MAGIC+1, version: VERSION+1, ..BucketHeader::default() };
            bincode::serialize_into(&mut tmp, &header).expect("bincode");
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            assert!(match bucket.check_headers() {
                Err(Error::BadMagic) => true,
                Err(Error::BadVersion) => true,
                _ => false
            });
        }

        // Invalid header (no bytes)
        {
            let tmp = NamedTempFile::new().unwrap();
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            assert!(bucket.check_headers().is_err());
        }

        // Invalid header (just magic)
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            bincode::serialize_into(&mut tmp, &MAGIC).expect("bincode");
            assert!(bucket.check_headers().is_err());
        }

        // Invalid header (magic + version)
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            bincode::serialize_into(&mut tmp, &MAGIC).expect("bincode");
            bincode::serialize_into(&mut tmp, &VERSION).expect("bincode");
            assert!(bucket.check_headers().is_err());
        }

        // Invalid header (magic + version + 32-bit time)
        {
            let mut tmp = NamedTempFile::new().unwrap();
            let bucket = Bucket::open(tmp.path()).expect("Bucket::open");
            bincode::serialize_into(&mut tmp, &MAGIC).expect("bincode");
            bincode::serialize_into(&mut tmp, &VERSION).expect("bincode");
            bincode::serialize_into(&mut tmp, &0_i32).expect("bincode");
            assert!(bucket.check_headers().is_err());
        }
    }

    proptest! {
        #[test]
        fn prop_create_si_zero(step in 1_usize .. 100) {
            let mut b = BTreeSet::new();
            let si = SparseIndex::new_with_step(step, &b);
            prop_assert!(si.index.is_empty());
        }
    }

    proptest! {
        #[test]
        fn prop_create_si_one(step in 1_usize .. 100) {
            let mut b = BTreeSet::new();
            b.insert(1);
            let si = SparseIndex::new_with_step(step, &b);
            prop_assert_eq!(si.index.len(), 2);
            prop_assert_eq!(si.index[0].key, si.index[1].key);
            prop_assert_eq!(si.index[0].offset, si.index[1].offset);
            prop_assert_eq!(si.index[0].offset, 0);
        }
    }

    proptest! {
        #![proptest_config(Config::with_cases(1000))]
        #[test]
        fn prop_create_si_two_and_more(len in 2_u64 .. 1000, step in 1_usize .. 100) {
            let mut b = BTreeSet::new();
            for i in 0 .. len {
                b.insert(i);
            }

            let si = SparseIndex::new_with_step(step, &b);
            prop_assert!(si.index.len() >= 2);
            prop_assert_eq!(si.index[0].key, 0);
            prop_assert_eq!(si.index[si.index.len() - 1].key, len-1);
            for i in 0 .. si.index.len() - 1 {
                prop_assert!(si.index[i].key < si.index[i+1].key);
                prop_assert!(si.index[i].offset < si.index[i+1].offset);
                prop_assert_eq!(si.index[i].offset,
                                (i * si.step * INDEX_ENTRY_SIZE) as u64);
            }
        }
    }


    proptest! {
        #[test]
        fn prop_try_get_all_present(len in 0_u64 .. 1000, step in 1_usize .. 2000) {
            let mut b = BTreeSet::new();
            for key in 0 .. len {
                b.insert(key);
            }

            let si = SparseIndex::new_with_step(step, &b);

            for key in 0 .. len {
                prop_assert!(si.try_get(key).is_some());
            }
        }
    }

    proptest! {
        #[test]
        fn prop_try_get_some_present(len in 0_u64 .. 1000, step in 1_usize .. 2000) {
            let mut b= BTreeSet::new();
            let mut max = 0;
            for key in (0 .. len).step_by(3) {
                b.insert(key);
                max = key;
            }

            let si = SparseIndex::new_with_step(step, &b);
            for key in 0 .. max {
                prop_assert!(si.try_get(key).is_some());
            }
        }
    }


    #[test]
    fn sparse_index_get() {
        {
            let si = SparseIndex::default();
            assert!(si.try_get(0).is_none());
        }

        {
            let mut si = SparseIndex::default();
            si.index = vec![IndexEntry { key: 1, offset: 1 }];
            assert!(si.try_get(0).is_none());
        }

        {
            let mut si = SparseIndex::default();
            si.index = vec![
                IndexEntry { key: 1, offset: 1 },
                IndexEntry { key: 4, offset: 4 },
            ];
            assert_matches!(si.try_get(0), None);
            assert_matches!(si.try_get(8), None);
            assert_matches!(si.try_get(1), Some((1, 1)));
            assert_matches!(si.try_get(4), Some((4, 4)));
            assert_matches!(si.try_get(3), Some((1, 4)));
        }
    }

    proptest! {
        #[test]
        fn prop_create_bucket_with_gaps(len in 0_usize .. 50, step in 1_usize .. 100) {
            use std::iter::FromIterator;

            let mut bmap = BTreeMap::new();
            let mut max = 0;
            for key in (0 .. len).step_by(step) {
                let key = key as u64;
                bmap.insert(key, BTreeSet::from_iter(0 .. (key as u128)));
                max = key;
            }

            let tmp = NamedTempFile::new().unwrap();
            create(tmp.path(), &bmap).expect("create");

            // Keys that exist
            {
                let bucket = Bucket::open(tmp.path()).expect("open");
                let mut bucket = bucket.check_headers().expect("check_headers");
                let si = bucket.read_sparse_index().expect("sparse index");

                for (key, actual_values) in &bmap {
                    let key = *key as u64;
                    let (offset_1, offset_2) = si.try_get(key).expect("try_get (1)");
                    let values = bucket.try_get(key, offset_1, offset_2)
                        .expect("try_get (1)")
                        .expect("try_get (2)");
                    prop_assert_eq!(actual_values, &values);
                }
            }

            // Keys that don't exist
            {
                let bucket = Bucket::open(tmp.path()).expect("open");
                let mut bucket = bucket.check_headers().expect("check_headers");
                let si = bucket.read_sparse_index().expect("sparse index");

                for key in 0 .. max {
                    if bmap.contains_key(&key) {
                        continue;
                    }
                    let (offset_1, offset_2) = si.try_get(key).expect("try_get (2)");
                    let values_opt = bucket.try_get(key, offset_1, offset_2).expect("try_get (1)");
                    prop_assert!(values_opt.is_none());
                }
            }
        }
    }

    proptest! {
        #[test]
        fn prop_merge_all(len_1 in 0_usize .. 50, len_2 in 0_usize .. 50) {
            use std::iter::FromIterator;

            let mut bmap1 = BTreeMap::new();
            for key in 0 .. len_1 {
                bmap1.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
            }

            let mut bmap2 = BTreeMap::new();
            for key in 0 .. len_2 {
                bmap2.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
            }

            let tmp1 = NamedTempFile::new().unwrap();
            let tmp2 = NamedTempFile::new().unwrap();
            let merged_file = NamedTempFile::new().unwrap();

            create(tmp1.path(), &bmap1).expect("create");
            create(tmp2.path(), &bmap2).expect("create");
            merge(tmp1.path(), tmp2.path(), merged_file.path()).expect("merge");

            // union bmap1 & bmap2
            for (key, values) in bmap1.iter() {
                let set = bmap2.entry(*key).or_insert(BTreeSet::new());
                let union: BTreeSet<Value> = set.union(values).cloned().collect();
                bmap2.insert(*key, union);
            }

            let mut merged = Bucket::open(merged_file).expect("open").check_headers().expect("headers");

            let si = merged.read_sparse_index().expect("read_sparse_index");
            for (key, values) in bmap2.iter()  {
                let (data_off_1, data_off_2) = si.try_get(*key).expect("try_get");
                let merged_values = merged.try_get(*key, data_off_1, data_off_2).expect("try_get");
                assert_eq!(*values, merged_values.expect("try_get"));
            }
        }
    }

    proptest! {
        #[test]
        fn prop_create_bucket_all(len in 0_usize .. 50) {
            use std::iter::FromIterator;

            let mut bmap = BTreeMap::new();
            for key in 0 .. len {
                bmap.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
            }

            let tmp = NamedTempFile::new().unwrap();
            create(tmp.path(), &bmap).expect("create");
            std::fs::copy(&tmp, "/tmp/saved.binstore").expect("copy");

            {
                let bucket = Bucket::open(tmp.path()).expect("open");
                let mut bucket = bucket.check_headers().expect("check_headers");
                let si = bucket.read_sparse_index().expect("sparse index");

                // Keys that exist
                for (key, actual_values) in &bmap {
                    let (offset_1, offset_2) = si.try_get(*key).expect("try_get");
                    let values = bucket.try_get(*key, offset_1, offset_2)
                        .expect("try_get (1)")
                        .expect("try_get (2)");
                    prop_assert_eq!(actual_values, &values);
                }

                // Keys that don't exist
                for key in len .. 2*len {
                    let key = key as u64;
                    let opt = si.try_get(key);
                    prop_assert!(opt.is_none());
                }
            }
        }
    }

    proptest! {
        #[test]
        fn prop_delete_some(len in 0_usize..50) {
            use std::iter::FromIterator;
            use rand::Rng;

            let mut bmap = BTreeMap::new();
            for key in 0..len {
                bmap.insert(key as HashedKey, BTreeSet::from_iter(0 .. (key as Value)));
            }

            // generate a random set of values to be deleted
            let mut bset = BTreeSet::new();
            let mut rng = rand::thread_rng();
            // if len = 0, rng.gen_range(0,0) will cause a panic
            if len != 0 {
                let number_of_values_to_delete = rng.gen_range(0, len);
                for _ in 0..number_of_values_to_delete {
                    bset.insert(rng.gen_range(0, len) as Value);
                }
            }

            let tmp = NamedTempFile::new().unwrap();
            let deleted = NamedTempFile::new().unwrap();
            create(tmp.path(), &bmap).expect("create");

            for (_, values) in bmap.iter_mut() {
                *values = values.difference(&bset).cloned().collect();
            }

            let bmap: BTreeMap<HashedKey, BTreeSet<Value>> = bmap.into_iter().filter(|(_, values)| !values.is_empty()).collect();

            let v: Vec<Value> = bset.into_iter().collect();
            delete(tmp.path().to_str().unwrap(), deleted.path().to_str().unwrap(), &v).expect("delete");

            {
                let bucket = Bucket::open(deleted.path()).expect("open");
                let mut bucket = bucket.check_headers().expect("check_headers");
                let si = bucket.read_sparse_index().expect("sparse index");

                for (key, actual_values) in &bmap {
                    let (offset_1, offset_2) = si.try_get(*key).expect("try_get");
                    let values = bucket.try_get(*key, offset_1, offset_2).expect("try_get").unwrap();
                    assert_eq!(values, *actual_values);
                }
            }
        }
    }

    #[test]
    fn bucketheader_size() {
        const HEADER_SIZE: usize = mem::size_of::<BucketHeader>();

        assert_eq!(
            HEADER_SIZE as u64,
            bincode::serialized_size(&BucketHeader::default()).unwrap()
        );
    }
}
