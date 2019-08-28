mod subcommands;
mod custom_logger;

use clap::{App, Arg, SubCommand, crate_name, crate_version};

fn main() {
    custom_logger::init();
    let app = App::new(crate_name!())
        .version(crate_version!())
        .subcommand(SubCommand::with_name("json-dump")
                    .about("Dump a bucket in JSON")
                    .arg(Arg::with_name("input-files")
                         .help("the list of files to accumulate; use `-` for stdin.")
                         .value_name("FILES")
                         .takes_value(true)
                         .multiple(true)))
        .subcommand(SubCommand::with_name("query-bucket")
                    .about("Queries a single bucket file to find if the provided key exists or not.")
                    .arg(Arg::with_name("key")
                        .help("The key used to retrieve the value in the database")
                        .short("k")
                        .long("key")
                        .value_name("KEY")
                        .takes_value(true)
                        .multiple(true))
                    .arg(Arg::with_name("input-files")
                         .help("the list of buckets to search in.")
                         .required(true)
                         .value_name("FILES")
                         .takes_value(true)
                         .multiple(true)))
        .subcommand(SubCommand::with_name("merge")
                    .about("Merges two buckets together, leaving the two original files intact.")
                    .arg(Arg::with_name("input-files")
                         .help("the two files to merge together.")
                         .required(true)
                         .value_name("FILES")
                         .takes_value(true)
                         .multiple(true))
                    .arg(Arg::with_name("output-name")
                        .help("the name of the output file")
                        .required(true)
                        .short("o")
                        .long("output-name")
                        .value_name("OUTPUT-NAME")
                        .takes_value(true)
                        .multiple(false)))
        .subcommand(SubCommand::with_name("delete")
                    .about("Duplicates the input files without including the provided values")
                    .arg(Arg::with_name("values")
                        .help("the values to remove from the database")
                        .short("v")
                        .long("values")
                        .value_name("VALUES")
                        .takes_value(true)
                        .multiple(true))
                    .arg(Arg::with_name("input-files")
                         .help("the list of files to delete in.")
                         .required(true)
                         .value_name("INPUT-FILES")
                         .takes_value(true)
                         .multiple(true))
                    .arg(Arg::with_name("output-files")
                         .help("the names of the output files in the same order as the input file")
                         .short("o")
                         .long("output")
                         .required(true)
                         .value_name("OUTPUT_FILES")
                         .takes_value(true)
                         .multiple(true)))
        .subcommand(SubCommand::with_name("query")
                    .about("Queries the database to retrieve the values associated with the provided key")
                    .arg(Arg::with_name("dbdir")
                        .short("-d")
                        .long("--db-dir")
                        .takes_value(true)
                        .default_value(".")
                        .value_name("DIR")
                        .help("root of the directory where the buckets are stored"))
                    .arg(Arg::with_name("key")
                        .help("the key to search for in the database")
                        .short("k")
                        .long("key")
                        .value_name("KEY")
                        .takes_value(true)
                        .multiple(true))
                    .arg(Arg::with_name("start-date")
                         .help("format: %Y-%m-%d")
                         .short("-s")
                         .long("--start-date")
                         .takes_value(true))
                    .arg(Arg::with_name("end-date")
                         .help("format %Y-%m-%d")
                         .short("-e")
                         .long("--end-date")
                         .takes_value(true)));

    let matches = app.get_matches();
    match matches.subcommand() {
        ("json-dump", Some(matches)) => subcommands::json_dump::main(matches),
        ("query-bucket", Some(matches)) => subcommands::query_bucket::main(matches),
        ("merge", Some(matches)) => subcommands::merge::main(matches),
        ("delete", Some(matches)) => subcommands::delete::main(matches),
        ("query", Some(matches)) => subcommands::query::main(matches),
        _ => {
            println!("{}", matches.usage());
        }
    }
}
