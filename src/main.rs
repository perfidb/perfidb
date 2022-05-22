mod db;
mod csv_reader;
mod transaction;

use std::io;
use std::path::Path;
use clap::{Parser};
use comfy_table::Table;
use rustyline::Editor;
use rustyline::error::ReadlineError;
use sqlparser::ast::{CopyTarget, Expr, Query, SetExpr, Statement};
use crate::db::Database;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::ParserError;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    /// Database file path
    file: String,
}

fn execute_copy(db : &mut Database, table_name :&str, target: &CopyTarget) {
    match target {
        CopyTarget::File { filename} => {
            let path = Path::new(filename);
            let result = csv_reader::read_transactions(table_name, path);
            match result {
                Ok(transactions) => {
                    for t in transactions {
                        db.upsert(&t);
                        println!("{:?}", t);
                    }
                    db.save();
                },
                Err(e) => {
                    println!("{}", e);
                }
            }
        },
        _ => {
            println!("{:?}", target);
        }
    }
}

fn execute_insert(db : &Database, table_name :&str, values: &Vec<Vec<Expr>>) {
    for v in values {
        println!("{:?}", v);
    }
}

fn parse_and_run_sql(db: &mut Database, sql: String) -> Result<(), ParserError> {
    let dialect = GenericDialect {};
    let sql_parse_result = sqlparser::parser::Parser::parse_sql(&dialect, sql.as_str());

    match sql_parse_result {
        Err(e) => {
            return Err(e);
        },
        Ok(ast) => {
            for statement in ast {
                match statement {
                    Statement::Query(query) => {
                        let Query { with, body, .. } = *query;
                        if let SetExpr::Select(select) = body {
                            println!("{:?}", select.projection);
                            println!("{:?}", select.from);

                            let mut table = Table::new();
                            table.set_header(vec!["Account", "Date", "Description", "Amount", "Tags"]);
                            for t in db.iter() {
                                // TODO handle tags
                                table.add_row(vec![t.account.as_str(), t.date.to_string().as_str(), t.description.as_str(), t.amount.to_string().as_str(), ""]);
                            }

                            println!("{table}");
                        }

                    },
                    Statement::Insert { table_name, source, .. } => {
                        println!("{:?}", table_name);

                        // Grab index 0 for now. TODO: make it nicer
                        let table_name :&str = table_name.0[0].value.as_str();

                        match source.body {
                            SetExpr::Values(values) => {
                                execute_insert(&db, table_name, &values.0);
                            },
                            _ => ()
                        }
                    },
                    Statement::Copy { table_name, target, .. } => {
                        // Grab index 0 for now. TODO: make it nicer
                        let table_name :&str = table_name.0[0].value.as_str();

                        execute_copy(db, table_name, &target);
                    },
                    _ => {

                    }
                }
            }
        }
    }

    Ok(())
}

static COMMAND_HISTORY_FILE: &str = ".transdb_history";
fn main() {
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
                let is_last = line.ends_with(";");
                sql_buffer.push(line);
                if is_last {
                    let sql = sql_buffer.join("\n");
                    rl.add_history_entry(sql.trim());
                    let result = parse_and_run_sql(&mut db, sql);
                    if let Err(err) = result {
                        println!("{}", err);
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
