use clap::{ArgMatches, values_t};
use binstore::db::*;
use binstore::prelude::*;
use std::process;
use chrono::*;

pub fn main(matches: &ArgMatches) {
    let dbdir = matches.value_of("dbdir").unwrap();
    let start_date_str = matches.value_of("start-date").unwrap();
    let end_date_str = matches.value_of("end-date").unwrap();

    let start_date = match parse_date(start_date_str) {
        Ok(date) => date,
        Err(e) => {
            eprintln!("binstore: cannot parse start date: {}", e);
            process::exit(1);
        }
    };

    let end_date = match parse_date(end_date_str) {
        Ok(date) => date,
        Err(e) => {
            eprintln!("binstore: cannot parse end date: {}", e);
            process::exit(1);
        }
    };

    let hashes: Vec<HashedKey> = match values_t!(matches, "hash", HashedKey) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("binstore: invalid hash: {}", e);
            process::exit(1);
        }
    };

    let path = std::path::PathBuf::from(dbdir);

    let mut ret = 0;
    match Db::open(path) {
        Ok(mut db) => {
            for hash in &hashes {
                match db.query(*hash, start_date, end_date) {
                    Ok(tifas) => {
                        println!("{}: {:?}", hash, tifas);
                    },
                    Err(e) => {
                        eprintln!("Jenny: {}", e);
                        ret = 1;
                    }
                }
            }
        },
        Err(e) => {
            eprintln!("Jenny: could not open database: {}", e);
            process::exit(1);
        }
    }

    process::exit(ret);
}

fn parse_date(s: &str) -> Result<Date<Local>> {
    let naive = NaiveDate::parse_from_str(s, "%Y-%m-%d")?;
    match TimeZone::from_local_date(&Local, &naive) {
        LocalResult::Single(date) => Ok(date),
        _ => Err(Error::DateParseError)
    }
}

