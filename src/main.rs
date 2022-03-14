mod db;
mod reader;
mod transaction;

use std::io;
use clap::{Parser, Subcommand};
use crate::db::Database;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List all transactions
    List {},
    /// Add transactions into database
    Upsert {
        /// Serialised database file path, in json format
        file: String
    },
}

fn main() {
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    match &cli.command {
        Commands::List {} => {},
        Commands::Upsert { file } => {
            let mut db = Database::load(file.as_str());
            let transactions = reader::read_transactions(io::stdin());
            for t in transactions {
                db.upsert(&t);
            }
            db.save_and_close();
        }
    }
}
