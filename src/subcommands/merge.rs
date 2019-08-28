use clap::{ArgMatches, values_t};
use std::process;

pub fn main(matches: &ArgMatches) {
    let filenames = values_t!(matches, "input-files", String).unwrap_or_else(|_| {
        eprintln!("binstore: missing input file");
        process::exit(1);
    });

    let output_name = values_t!(matches, "output-name", String).unwrap_or_else(|_| {
        eprintln!("binstore: missing output name");
        process::exit(1);
    });

    if filenames.len() != 2 {
        eprintln!("binstore: exactly two filenames must be provided");
        process::exit(1);
    }

    if let Err(e) = binstore::bucket::merge(&filenames[0], &filenames[1], &output_name[0]) {
        eprintln!("binstore: {}", e);
        process::exit(1);
    }
 }

