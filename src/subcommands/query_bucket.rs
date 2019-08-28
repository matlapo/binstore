use clap::{ArgMatches, values_t};
use binstore::error::*;
use binstore::bucket::*;
use binstore::prelude::*;
use log::debug;
use std::process;
use std::time::Instant;

pub fn main(matches: &ArgMatches) {
    let hashes: Vec<HashedKey> = match values_t!(matches, "hash", HashedKey) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("binstore: invalid hash: {}", e);
            process::exit(1);
        }
    };

    let filenames: Vec<String> = match values_t!(matches, "input-files", String) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("binstore: invalid input file: {}", e);
            process::exit(1)
        }
    };

    let mut ret = 0;
    for filename in &filenames {
        if let Err(e) = multi_query(filename, &hashes) {
            ret = 1;
            eprintln!("binstore: {}: {}", filename, e);
        }
    }
    process::exit(ret);
}

fn multi_query(filename: &str, hashes: &[HashedKey]) -> Result<()> {
    let t = Instant::now();
    let bucket = Bucket::open(filename)?;
    debug!("opened {} in {:?}", filename, t.elapsed());

    let t = Instant::now();
    let mut bucket = bucket.check_headers()?;
    debug!("checked headers in {:?}", t.elapsed());

    let t = Instant::now();
    let si = bucket.read_sparse_index()?;
    debug!("read sparse index in {:?}", t.elapsed());

    for hash in hashes {
        let t = Instant::now();
        let maybe_range = si.try_get(*hash);
        debug!("sparse index lookup: {:?}", t.elapsed());
        if let Some((off1, off2)) = maybe_range {
            let v = bucket.try_get(*hash, off1, off2)?;
            println!("{}: {}: {:?}", filename, *hash, v);
        }
        debug!("searched key {} in {:?}", hash, t.elapsed());
    }

    return Ok(());
}
