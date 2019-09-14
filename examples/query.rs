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