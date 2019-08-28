use chrono::prelude::*;
use crate::prelude::*;
use crate::bucket::*;
use log::{debug, warn};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// A Database consists of multiple buckets; each indexed by a Date.
pub struct Db {
    buckets: BTreeMap<Date<Local>, Bucket<Checked>>,
    pub root: PathBuf,
}

impl Db {
    pub fn new<P: AsRef<Path>>(root: P) -> Db {
        Db {
            buckets: BTreeMap::new(),
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn open<P: AsRef<Path>>(root: P) -> Result<Db> {
        use std::fs::*;
        let mut db = Db::new(root.as_ref());
        let entries = read_dir(root.as_ref()).expect("root is not a directory!");
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                let bucket = Bucket::open(&path)?;
                match bucket.check_headers() {
                    Ok(bucket) => {
                        let datetime = Local.timestamp(bucket.header.timestamp, 0);
                        db.buckets.insert(datetime.date(), bucket);
                    },
                    Err(e) => {
                        warn!("could not load bucket from file {:?} with error: {}", &path, e);
                    }
                }
            }
        }
        Ok(db)
    }

    pub fn query(&mut self, hash: HashedKey, start_date: Date<Local>, end_date: Date<Local>) -> Result<Vec<Value>> {
        let range = self.buckets.range_mut(start_date ..= end_date);
        let mut v = Vec::new();

        for (date, bucket) in range {
            debug!("querying bucket for date: {} with hash: {}", date, hash);
            match bucket.get(hash)? {
                Some(set) => {
                    for e in set {
                        v.push(e);
                    }
                },
                None => ()
            };
        }
        Ok(v)
    }

    pub fn len(&self) -> usize {
        self.buckets.len()
    }
}
