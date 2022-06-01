mod query;

use std::path::Path;
use sqlparser::ast::{CopyOption, CopyTarget, Expr, SetExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::ParserError;
use crate::{csv_reader, Database};
use crate::sql::query::run_query;

fn execute_copy(db : &mut Database, table_name :&str, target: &CopyTarget, inverse_amount: bool) {
    match target {
        CopyTarget::File { filename} => {
            let path = Path::new(filename);
            let result = csv_reader::read_transactions(table_name, path, inverse_amount);
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
                    _ => {

                    }
                }
            }
        }
    }

    Ok(())
}