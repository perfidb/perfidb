use std::{fs, process};
use std::path::PathBuf;
use clap::Parser;
use env_logger::Env;
use log::{error, info};
use rustyline::Editor;
use rustyline::error::ReadlineError;
use toml::Value;

extern crate dirs;

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
#[command(author, version, about)]
struct Cli {
    /// Database file path. If not specified it will be created at ~/.perfidb/finance.db
    #[arg(short, long, value_name = "DATABASE_FILE")]
    file: Option<String>,

    /// A toml file containing auto labelling regex. By default perfidb will try look for '~/.peridb/auto_label_rules.toml' file.
    /// An example toml file is generated in '~/.perfidb' directory. Remove '.example' suffix to start using this file.
    #[arg(short, long = "auto-label-rules", value_name = "TOML_PATH")]
    auto_label_rules_file: Option<String>,
}

static COMMAND_HISTORY_FILE: &str = ".transdb_history";
fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let cli :Cli = Cli::parse();

    let mut db= init_and_load_database(&cli.file);
    let auto_label_rules_file = match &cli.auto_label_rules_file {
        Some(f) => f.clone(),
        None => {
            let user_home_dir = dirs::home_dir().unwrap();
            user_home_dir.join(".perfidb").join("auto_label_rules.toml").as_path().display().to_string()
        }
    };

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
                        let result = parse_and_run_sql(&mut db, sql, auto_label_rules_file.as_str());
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

fn init_and_load_database(file_from_cli: &Option<String>) -> Database {
    if let Some(file_from_cli) = file_from_cli {
        info!("Loading database from {}", file_from_cli);
        Database::load(file_from_cli)
    } else {
        let mut perfidb_home_dir = dirs::home_dir().unwrap();
        perfidb_home_dir.push(".perfidb");
        if perfidb_home_dir.exists() && perfidb_home_dir.is_file() {
            error!("{} already exists and is not a directory. Please remove this file and re-run perfidb", perfidb_home_dir.display());
            process::exit(1);
        }

        if !perfidb_home_dir.exists() {
            fs::create_dir(&perfidb_home_dir).unwrap();
            create_auto_label_rules_example(&perfidb_home_dir);
        }

        let db_file = perfidb_home_dir.join("finance.db");
        if !db_file.exists() {
            info!("Creating database file in $HOME/.perfidb/finance.db");
            let db = Database::new(db_file.as_path().display().to_string());
            db.save();
        }

        Database::load(db_file.as_path().to_str().unwrap())
    }
}

fn create_auto_label_rules_example(perfidb_home_dir: &PathBuf) {
    let mut config = Config::empty();
    config.labels.insert("grocery".to_string(), Value::Array(vec![Value::String("woolworths".to_string()), Value::String("coles".to_string())]));
    config.labels.insert("transfer".to_string(), Value::Array(vec![Value::String("^DIRECT DEBIT RECEIVED - THANK YOU".to_string())]));

    let toml_text = toml::to_string(&config).unwrap();
    fs::write(perfidb_home_dir.join("auto_label_rules.toml.example"), toml_text).expect("Could not create auto_label_rules.toml.example");
}