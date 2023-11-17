use std::{fs, process};

use std::path::{Path, PathBuf};
use clap::Parser;
use env_logger::Env;
use log::{debug, error, info};
use rustyline::error::ReadlineError;
use toml::Value;
use std::io::Write;
use rustyline::{Cmd, CompletionType, EditMode, KeyEvent};
use rustyline::completion::FilenameCompleter;
use rustyline::highlight::MatchingBracketHighlighter;
use rustyline::hint::HistoryHinter;
use rustyline::validate::MatchingBracketValidator;

extern crate dirs;
extern crate core;

use crate::config::Config;
use crate::db::Database;
use crate::sql::parse_and_run_sql;

mod common;
mod db;
mod csv_reader;
mod transaction;
mod sql;
mod config;
mod labeller;
mod live_edit;
mod editor;
mod util;
mod import;

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    /// Database file path. If not specified it will be created at ~/.perfidb/finance.db
    #[arg(short, long, value_name = "DATABASE_FILE")]
    file: Option<String>,

    /// The dir that contains bank transaction csv files
    #[arg(short, long, value_name = "IMPORT_ROOT_DIR")]
    import_root_dir: String,

    /// A toml file containing auto labelling regex. By default perfidb will try look for '~/.peridb/auto_label_rules.toml' file.
    /// An example toml file is generated in '~/.perfidb' directory. Remove '.example' suffix to start using this file.
    #[arg(short, long = "auto-label-rules", value_name = "TOML_PATH")]
    auto_label_rules_file: Option<String>,
}

static COMMAND_HISTORY_FILE: &str = ".perfidb_history";

static WELCOME_MESSAGE: &'static str = r#"
+-----------------------------------------------------+
|                                                     |
| Welcome to PerfiDB                                  |
|                                                     |
| For usage visit: https://github.com/perfidb/perfidb |
| To exit, use command 'exit'                         |
+-----------------------------------------------------+
"#;

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        // Remove logger formatting
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .init();

    let cli :Cli = Cli::parse();

    info!("{}", WELCOME_MESSAGE);

    let import_root_dir = PathBuf::from(cli.import_root_dir);
    let mut db = init_and_load_database(&cli.file, &import_root_dir);
    let auto_label_rules_file = match &cli.auto_label_rules_file {
        Some(f) => f.clone(),
        None => {
            let user_home_dir = dirs::home_dir().unwrap();
            user_home_dir.join(".perfidb").join("auto_label_rules.toml").as_path().display().to_string()
        }
    };


    let config = rustyline::Config::builder()
        .history_ignore_space(true)
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Emacs)
        .build();
    let helper = editor::PerfidbHelper {
        completer: FilenameCompleter::new(),
        highlighter: MatchingBracketHighlighter::new(),
        hinter: HistoryHinter {},
        colored_prompt: "# ".to_owned(),
        validator: MatchingBracketValidator::new(),
    };
    let mut rl = rustyline::Editor::with_config(config).expect("Unable to create terminal editor");
    rl.set_helper(Some(helper));
    rl.bind_sequence(KeyEvent::alt('n'), Cmd::HistorySearchForward);
    rl.bind_sequence(KeyEvent::alt('p'), Cmd::HistorySearchBackward);

    let command_history_file: PathBuf = perfidb_home_path().join(COMMAND_HISTORY_FILE);

    if rl.load_history(command_history_file.as_path()).is_err() {
        debug!("No previous command history found.");
    }


    let mut sql_buffer :Vec<String> = vec![];
    loop {
        let readline = rl.readline("# ");
        match readline {
            Ok(line) => {
                let line = line.trim();

                // Check if line is a control command
                if sql_buffer.is_empty() {
                    match line.to_ascii_lowercase().as_str() {
                        "exit" => {
                            info!("\nBye!\n");
                            break;
                        },
                        "live" => {
                            if let Some(last_results) = &db.last_query_results {
                                live_edit::live_label(last_results.clone(), &mut db, &auto_label_rules_file).unwrap();
                            } else {
                                info!("No recent select results");
                            }
                            continue;
                        }
                        _ => {}
                    }
                }

                let is_last = line.ends_with(';');
                if !line.is_empty() {
                    sql_buffer.push(line.to_string());
                }
                if is_last {
                    let sql = sql_buffer.join("\n");
                    let _ = rl.add_history_entry(sql.trim());

                    // Remove leading and trailing space and semicolon
                    let pattern :&[_] = &[' ', ';'];
                    let sql = sql.trim_matches(pattern).to_string();
                    let result = parse_and_run_sql(&mut db, &import_root_dir, sql, &auto_label_rules_file);

                    if let Err(err) = result {
                        error!("{}", err);
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
    rl.save_history(command_history_file.as_path()).unwrap();
}

fn perfidb_home_path() -> PathBuf {
    let user_home = dirs::home_dir().expect("Unable to locate user HOME dir");
    user_home.join(".perfidb")
}

fn init_and_load_database(file_from_cli: &Option<String>, _import_root_dir: &PathBuf) -> Database {
    if let Some(file_from_cli) = file_from_cli {
        info!("Loading database from: {}", file_from_cli);
        Database::load(file_from_cli).unwrap()
    } else {
        let perfidb_home_dir = perfidb_home_path();
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
            let db_file_path = db_file.as_path().display().to_string();
            info!("Creating database file in {}", db_file_path);
            let db = Database::new(db_file_path);
            db.save();
        }

        Database::load(db_file.as_path().to_str().unwrap()).unwrap()
    }
}

fn create_auto_label_rules_example(perfidb_home_dir: &Path) {
    let mut config = Config::empty();
    config.labels.insert("grocery".to_string(), Value::Array(vec![Value::String("woolworths".to_string()), Value::String("coles".to_string())]));
    config.labels.insert("transfer".to_string(), Value::Array(vec![Value::String("^DIRECT DEBIT RECEIVED - THANK YOU".to_string())]));

    let toml_text = toml::to_string(&config).unwrap();
    fs::write(perfidb_home_dir.join("auto_label_rules.toml.example"), toml_text).expect("Could not create auto_label_rules.toml.example");
}
