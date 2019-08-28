use clap::{ArgMatches, values_t};
use binstore::prelude::*;
use binstore::bucket;
use serde::Serialize;
use std::collections::BTreeSet;
use std::io::{self, BufWriter, stdout, Write};
use std::io::{SeekFrom, Seek};
use lz4::{Decoder};

pub fn main(matches: &ArgMatches) {
    let filenames = values_t!(matches, "input-files", String).unwrap_or(vec![]);
    let mut ret = 0;
    for filename in filenames {
        match dump(&filename) {
            Ok(()) => { }
            Err(e) => {
                eprintln!("binstore: {}", e);
                ret = 1;
            }
        }
    }
    std::process::exit(ret);
}

struct LargeNumberAsStrings;

impl serde_json::ser::Formatter for LargeNumberAsStrings {
    fn write_u64<W: Write + ?Sized>(&mut self, w: &mut W, value: u64) -> std::io::Result<()> {
        write!(w, r#""{}""#, value)
    }

    fn write_number_str<W: Write + ?Sized>(&mut self, w: &mut W, s: &str) -> std::io::Result<()> {
        write!(w, r#""{}""#, s)
    }
}

#[derive(Serialize)]
struct ValueEntry {
    key: HashedKey,
    absolute_offset: u64,
    values: BTreeSet<Value>,
}

fn dump(filename: &str) -> Result<()> {
    let stdout = stdout();
    let stdout = stdout.lock();
    let mut stdout = BufWriter::new(stdout);

    let mut json_serializer = serde_json::Serializer::with_formatter(&mut stdout, LargeNumberAsStrings{});

    let bucket = bucket::Bucket::open(filename)?;
    let mut bucket = bucket.check_headers()?;

    // Dump header
    bucket.header.serialize(&mut json_serializer)?;

    // Dump sparse index
    let si: bucket::SparseIndex = bincode::deserialize_from(&mut bucket.file)?;
    si.serialize(&mut json_serializer)?;

    // Dump dense index
    let num_entries =
        (bucket.header.data_base_offset - bucket.header.di_base_offset) / (bucket::INDEX_ENTRY_SIZE as u64);

    for _ in 0 .. num_entries {
        // Decode Dense Index entry
        let di_entry: bucket::IndexEntry = bincode::deserialize_from(&mut bucket.file)?;

        // Save current position
        let curr_pos = bucket.file.seek(SeekFrom::Current(0))?;

        // Go to the offset where the values associated with this index entry are.
        let abs_offset = bucket.header.data_base_offset + di_entry.offset;
        bucket.file.seek(SeekFrom::Start(abs_offset))?;

        // Decode the lz4 payload.
        let mut bincode: Vec<u8> = Vec::new();
        let mut lz4_decoder = Decoder::new(&mut bucket.file)?;
        io::copy(&mut lz4_decoder, &mut bincode)?;
        let u8_ref: &[u8] = bincode.as_ref();
        let values: BTreeSet<Value> = bincode::deserialize_from(u8_ref)?;

        // Go back to where we came from.
        bucket.file.seek(SeekFrom::Start(curr_pos))?;

        let entry = ValueEntry {
            key: di_entry.key,
            absolute_offset: abs_offset,
            values: values
        };
        entry.serialize(&mut json_serializer)?;
    }

    return Ok(());
}
