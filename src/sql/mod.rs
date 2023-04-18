mod query;
mod insert;
mod util;
mod copy;
pub mod parser;

use log::{info, warn};
use sqlparser::ast::{Expr, Statement, TableFactor, Value, Function, FunctionArg, FunctionArgExpr};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::ParserError;
use crate::{Config, Database};
use crate::sql::query::run_query;
use crate::tagger::Tagger;

pub(crate) fn parse_and_run_sql(db: &mut Database, sql: String, auto_label_rules_file: &str) -> Result<(), ParserError> {
    // First use our own parser to parse
    let result = parser::parse(&sql);
    if let Ok(statement) = result {
        match statement {
            parser::Statement::Export(file_path) => {
                copy::execute_export_db(db, &file_path);
                return Ok(())
            },
            parser::Statement::Import(account, file_path, inverse_amount, dryrun) => {
                copy::execute_import(db, &account, &file_path, inverse_amount, dryrun);
                return Ok(())
            },
            parser::Statement::Select(projection, from, condition) => {
                query::select::run_select(db, projection, from, condition);
                return Ok(())
            }
            _ => ()
        }
    }

    // Now we fall back to sqlparser. We will gradually migrate sqlparser to our own nom based parser.
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
                        run_query(query, db, auto_label_rules_file);
                    },

                    Statement::Insert { table_name, source, .. } => {
                        insert::execute_insert(db, table_name, source);
                    },

                    Statement::Update { assignments, selection: Some(where_clause), .. } => {
                        for assignment in assignments {
                            if assignment.id[0].value == "label" {
                                match assignment.value {
                                    Expr::Value(Value::SingleQuotedString(labels)) => {
                                        let labels: Vec<&str> = labels.split(',').map(|t| t.trim()).collect();
                                        db.set_labels_for_multiple_transactions(&where_clause, &labels);
                                    },

                                    Expr::Function(func) => {
                                        // SET label = auto()
                                        if func.name.0[0].value.to_ascii_lowercase() == "auto" {
                                            let auto_labeller = Tagger::new(&Config::load_from_file(auto_label_rules_file));
                                            db.auto_label(&auto_labeller, &where_clause);
                                        }
                                    }
                                    _ => {
                                        warn!("\"{}\" is not a supported label value or function. Try:  SET label = 'grocery'", assignment.value);
                                    }
                                }
                            }
                        }
                    },

                    // If no 'FROM' clause, we assume updating on one transaction and that transaction id is table name
                    Statement::Update { table, assignments, selection: None, .. } => {
                        if let TableFactor::Table { name, .. } = table.relation {
                            let trans_id = name.0[0].value.parse::<u32>().unwrap();
                            for assignment in assignments {
                                if assignment.id[0].value == "label" {
                                    update_transaction_tags(db, trans_id, assignment.value);
                                }
                            }
                        }
                    },
                    _ => {
                        println!("Unsupported statement {statement:?}");
                    }
                }
            }
        }
    }

    Ok(())
}

pub(crate) fn update_transaction_tags(db: &mut Database, trans_id: u32, tag_value_expr: Expr) {
    if let Expr::Value(Value::SingleQuotedString(labels)) = tag_value_expr {
        db.update_labels(trans_id, &labels);
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