mod query;

use std::path::Path;
use sqlparser::ast::{Assignment, CopyOption, CopyTarget, Expr, SetExpr, Statement, TableFactor, Value};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::ParserError;
use crate::{csv_reader, Database};
use crate::sql::query::run_query;
use walkdir::WalkDir;

fn copy_from_csv(path: &Path, db: &mut Database, table_name: &str, inverse_amount: bool) {
    let result = csv_reader::read_transactions(table_name, path, inverse_amount);
    match result {
        Ok(transactions) => {
            for t in &transactions {
                db.upsert(t);
            }
            db.save();
            println!("Imported {} transactions", &transactions.len());
        },
        Err(e) => {
            println!("{}", e);
        }
    }

}

fn execute_copy(db : &mut Database, table_name :&str, target: &CopyTarget, inverse_amount: bool) {
    match target {
        CopyTarget::File { filename} => {
            let path = Path::new(filename);
            if path.is_dir() {
                for entry in WalkDir::new(path).into_iter() {
                    let dir_entry = entry.unwrap();
                    if dir_entry.path().is_file() {
                        println!("Copying from {}", dir_entry.path().display());
                        copy_from_csv(dir_entry.path(), db, table_name, inverse_amount);
                    }
                }
            } else if path.is_file() {
                println!("Copying from {}", path.display());
                copy_from_csv(path, db, table_name, inverse_amount);
            }
        },
        _ => {
            println!("{:?}", target);
        }
    }
}

fn execute_insert(_db : &Database, _table_name :&str, values: &Vec<Vec<Expr>>) {
    for v in values {
        println!("{:?}", v);
    }
}

pub(crate) fn parse_and_run_sql(db: &mut Database, sql: String) -> Result<(), ParserError> {
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
                        run_query(query, &db);
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

                    Statement::Copy { table_name, target, options, .. } => {
                        // Grab index 0 for now. TODO: make it nicer
                        let table_name :&str = table_name.0[0].value.as_str();

                        // should we inverse amount value
                        let mut inverse_amount = false;
                        for option in options {
                            if let CopyOption::Format(ident) = option {
                                let format_value = ident.value.to_lowercase();
                                if format_value == "i" || format_value == "inverse" {
                                    inverse_amount = true;
                                }
                            }
                        }

                        execute_copy(db, table_name, &target, inverse_amount);
                    },

                    Statement::Update { table, assignments, .. } => {
                        if let TableFactor::Table { name, .. } = table.relation {
                            let trans_id = name.0[0].value.parse::<u32>().unwrap();
                            for assignment in assignments {
                                if let Expr::Value(Value::SingleQuotedString(tags)) = assignment.value {
                                    let tags: Vec<&str> = tags.split(',').map(|t| t.trim()).collect();
                                    db.update_tags(trans_id, &tags);
                                }
                            }
                        }
                    },
                    _ => {
                        println!("Unsupported statement {:?}", statement);
                    }
                }
            }
        }
    }

    Ok(())
}