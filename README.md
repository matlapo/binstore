# Binstore [![crates.io](https://img.shields.io/crates/v/binstore)](https://crates.io/crates/binstore)

Binstore is a simple key-value store written in Rust. This means that serialization/deserialization is not handled by binstore. All it does is storing key-value elements in a cache-friendly and compact file format. For now, this project is mostly for fun, but could hopefully evolve into something useable in the future.

# Documentation

https://docs.rs/binstore/

# File format
## Headers

| Field name       | Description                      | Type |
| ---------------- |:---------------------------------|-----:|
| magic            | Magic number                     | u32  |
| version          | Version number                   | u8   |
| timestamp        | Creation timestamp               | i64  |
| si_base_offset   | Where the sparse index begins    | u64  |
| di_base_offset   | Where the dense index begins     | u64  |
| data_base_offset | Where the compressed data begins | u64  |
| num_entries      | Number of entries in file        | u64  |

## Sparse Index
| Key    | DI Offset |
|--------|-----------|
| h_0000 | di_off_1  |
| h_1000 | di_off_2  |
| h_2000 | di_off_3  |
| ...    | ...       |
| h_xxxx | di_off_x  |

## Dense Index
| DI Offset | Key    | Data Offset |
|-----------|--------|-------------|
| di_off_1  | h_0000 | data_off_1  |
|           | h_0001 | data_off_2  |
|           | h_0013 | data_off_3  |
| ...       | ...    | ...         |
|           | h_0988 | data_off_4  |
| di_off_2  | h_1000 | data_off_5  |
|           | h_1003 | data_off_6  |
| ...       | ...    | ...         |
| di_off_x  | h_xxxx | data_off_x  |

## Data
| Data Offset | Data  |
|-------------|-------|
| data_off_1  | LZ4_1 |
| data_off_2  | LZ4_2 |
| data_off_3  | LZ4_3 |
| ...         | ...   |
| data_off_x  | LZ4_x |

# Explanation
A binstore file is split in four sections:

1. The headers.  The headers help us identify a binstore file (via the
   magic number), they allow us to know whether the current binstore
   engine can read a given binstore file (version number), and when the
   binstore file was created.  We also store the offsets for the other
   sections and the number of entries stored in this data file.
2. The sparse index.  An index of no more than 1-2 MB; the sparse is
   used to jump into the dense index at roughly the spot where the key
   we are looking for is located.  The sparse index is essential to
   avoid a full scan.
3. The dense index.  In the dense index, there is a mapping from a key
   (keys are explained below) to the file offset where the set of
   `Value`s associated with that key is stored.  The dense index entries
   are of fixed sized and are ordered by their keys; this enables
   binary searching.
4. The data.  This is where the actual `Value`s are stored.  To save
   space, we use the LZ4 compression algorithm.
   
# Examples

## Query

```rust
use std::iter::FromIterator;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::NamedTempFile;
use binstore::bucket::*;

fn main() {
    let mut bmap = BTreeMap::new();
    for key in 0 .. 100 {
        bmap.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
    }

    let tmp = NamedTempFile::new().unwrap();
    create(tmp.path(), &bmap).expect("create");

    {
        let bucket = Bucket::open(tmp.path()).expect("open");
        let mut bucket = bucket.check_headers().expect("check_headers");
        let si = bucket.read_sparse_index().expect("sparse index");

        for (key, actual_values) in &bmap {
            let (offset_1, offset_2) = si.try_get(*key).expect("try_get");
            let values = bucket.try_get(*key, offset_1, offset_2)
                .expect("try_get (1)")
                .expect("try_get (2)");
            assert_eq!(actual_values, &values);
        }
    }
}
```

## Merge

```rust
use std::iter::FromIterator;
use std::collections::{BTreeMap, BTreeSet};
use tempfile::NamedTempFile;
use binstore::bucket::*;

fn main() {
    let mut bmap1 = BTreeMap::new();
    for key in 0 .. 100 {
        bmap1.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
    }

    let mut bmap2 = BTreeMap::new();
    for key in 0 .. 200 {
        bmap2.insert(key as u64, BTreeSet::from_iter(0 .. (key as u128)));
    }

    let tmp1 = NamedTempFile::new().unwrap();
    let tmp2 = NamedTempFile::new().unwrap();
    let merged_file = NamedTempFile::new().unwrap();

    create(tmp1.path(), &bmap1).unwrap();
    create(tmp2.path(), &bmap2).unwrap();
    merge(tmp1.path(), tmp2.path(), merged_file.path()).unwrap();
}
```

More examples will be added to `examples/` in the future.
