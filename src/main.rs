mod db;
mod reader;
mod transaction;

use std::io;
use clap::{Parser, Subcommand};
use comfy_table::Table;
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
    List {
        /// Serialised database file path, in json format
        file: String
    },
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
        Commands::List { file } => {
            let mut table = Table::new();
            table.set_header(vec!["Account", "Date", "Description", "Amount", "Tags"]);
            let mut db = Database::load(file.as_str());
            for t in db.iter() {
                // TODO handle tags
                table.add_row(vec![t.account.as_str(), t.date.to_string().as_str(), t.description.as_str(), t.amount.to_string().as_str(), ""]);
            }

            println!("{table}");
        },
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
