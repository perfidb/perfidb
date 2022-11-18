use clap::Parser;
use env_logger::Env;
use log::info;
use rustyline::Editor;
use rustyline::error::ReadlineError;

use crate::config::Config;
use crate::db::Database;
use crate::sql::parse_and_run_sql;

mod db;
mod csv_reader;
mod transaction;
mod sql;
mod config;
mod tagger;
mod live_edit;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// Database file path
    file: String,

    /// Auto labelling rules
    auto_label_rules_file: Option<String>,
}

static COMMAND_HISTORY_FILE: &str = ".transdb_history";
fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let cli :Cli = Cli::parse();

    let mut db= Database::load(cli.file.as_str());

    let mut rl = Editor::<()>::new();
    if rl.load_history(COMMAND_HISTORY_FILE).is_err() {
        println!("No previous history.");
    }
    let mut sql_buffer :Vec<String> = vec![];
    loop {
        let readline = rl.readline("# ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                let is_last = line.ends_with(';');
                if !line.is_empty() {
                    sql_buffer.push(line.to_string());
                }
                if is_last {
                    let sql = sql_buffer.join("\n");
                    rl.add_history_entry(sql.trim());

                    if sql == "active;" {
                        if let Some(last_results) = &db.last_query_results {
                            live_edit::live_label(last_results.clone(), &mut db).unwrap();
                        } else {
                            info!("No recent query results");
                        }
                    } else {
                        let auto_label_rules_file = match &cli.auto_label_rules_file {
                            Some(f) => f.as_str(),
                            None => ""
                        };
                        let result = parse_and_run_sql(&mut db, sql, auto_label_rules_file);
                        if let Err(err) = result {
                            println!("{}", err);
                        }
                    }

                    sql_buffer.clear();
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break
            },
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break
            },
            Err(err) => {
                println!("Error: {:?}", err);
                break
            }
        }
    }
    rl.save_history(COMMAND_HISTORY_FILE).unwrap();
}
