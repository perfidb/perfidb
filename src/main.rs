mod db;

use std::error::Error;
use std::io;
use std::io::stdin;
use clap::{Parser, Subcommand};
use crate::db::{Database, Transaction};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add transactions into database
    Upsert,
}

fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Commands::Upsert => {
            let mut db = Database::new();
            let transactions = read_csv().unwrap();
            for t in transactions {
                db.upsert(t);
            }

            for t in db.iter() {
                println!("{:?}", t);
            }
        }
    }
}


fn read_csv() -> Result<Vec<Transaction>, Box<dyn Error>> {
    let mut transactions :Vec<Transaction> = vec![];
    let mut rdr = csv::Reader::from_reader(io::stdin());
    for results in rdr.deserialize() {
        let t: Transaction = results?;
        transactions.push(t);
    }
    Ok(transactions)
}
