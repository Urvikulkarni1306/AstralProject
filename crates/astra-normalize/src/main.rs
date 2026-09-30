use std::path::PathBuf;
use std::process::ExitCode;

use astra_normalize::{NormalizeError, normalize};
use clap::Parser;

#[derive(Parser)]
#[command(name = "astra-normalize", version)]
#[command(about = "Normalize captures into Parquet datasets")]
struct Cli {
    #[arg(long, value_name = "DIR")]
    input: PathBuf,
    #[arg(long, value_name = "DIR")]
    output: PathBuf,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("astra-normalize: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), NormalizeError> {
    let summary = normalize(&cli.input, &cli.output)?;

    println!("input       {}", cli.input.display());
    println!("output      {}", cli.output.display());
    println!("records     {}", summary.records);
    println!("rows        {}", summary.rows_written);
    println!("skipped     {}", summary.skipped_unsupported_channel);
    for file in &summary.files {
        println!("file        {}", file.display());
    }

    Ok(())
}
