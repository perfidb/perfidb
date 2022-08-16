mod query;

use std::path::Path;
use log::{info, warn};
use sqlparser::ast::{CopyOption, CopyTarget, Expr, SetExpr, Statement, TableFactor, Value, Function, FunctionArg, FunctionArgExpr};
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

fn execute_insert(_db : &Database, _table_name :&str, values: &[Vec<Expr>]) {
    for _v in values {
        // TODO: implement single INSERT
        println!("INSERT statement is not implemented yet");
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
                        run_query(query, db);
                    },

                    Statement::Insert { table_name, source, .. } => {
                        println!("{:?}", table_name);

                        // Grab index 0 for now. TODO: make it nicer
                        let table_name :&str = table_name.0[0].value.as_str();

                        if let SetExpr::Values(values) = source.body {
                            execute_insert(db, table_name, &values.0);
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
                                if assignment.id[0].value == "tags" {
                                    update_transaction_tags(db, trans_id, assignment.value);
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

fn update_transaction_tags(db: &mut Database, trans_id: u32, tag_value_expr: Expr) {
    if let Expr::Value(Value::SingleQuotedString(tags)) = tag_value_expr {
        let tags: Vec<&str> = tags.split(',').map(|t| t.trim()).collect();
        db.update_tags(trans_id, &tags);
        return;
    }

    if let Expr::Function(Function { name, args, .. }) = tag_value_expr {
        if name.0[0].value == "remove" {
            let tags = extract_args_string(&args);
            db.remove_tags(trans_id, &tags);
        }
        return;
    }

    info!("Unable to parse tags value expr {:?}", tag_value_expr);
}

/// Extract string values from a list of FunctionArg.
/// Currently only support Ident and SingleQuotedString, ie. remove(grocery),  remove('grocery')
fn extract_args_string(args: &[FunctionArg]) -> Vec<&str> {
    let mut result = vec![];
    for arg in args {
        match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                result.push(ident.value.as_str());
            },
            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(string)))) => {
                result.push(string.as_str());
            },
            _ => {
                warn!("{:?} is not a supported function argument", arg);
            }
        }
    }

    result
}