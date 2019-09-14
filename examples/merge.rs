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