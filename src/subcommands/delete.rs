use clap::{ArgMatches, values_t};
use binstore::prelude::Value;
use std::process;

pub fn main(matches: &ArgMatches) {
    let input_files: Vec<String> = match values_t!(matches, "input-files", String) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("binstore: invalid input file: {}", e);
            process::exit(1)
        }
    };

    let output_files: Vec<String> = match values_t!(matches, "output-files", String) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("binstore: invalid output file: {}", e);
            process::exit(1)
        }
    };

    let values: Vec<Value> = match values_t!(matches, "values", Value) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("hydroxyde: invalid values: {}", e);
            process::exit(1);
        }
    };

    if input_files.len() != output_files.len() {
        eprintln!("binstore: number of input files does not match number of output files");
        process::exit(1)
    }

    let files: Vec<(String, String)> = input_files.into_iter().zip(output_files).collect();

    let mut ret = 0;
    for (input, output) in &files {
        if let Err(e) = binstore::bucket::delete(input, output, &values) {
            ret = 1;
            eprintln!("binstore: {}: {}", input, e);
        }
    }
    process::exit(ret);
}
