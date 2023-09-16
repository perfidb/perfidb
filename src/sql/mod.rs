mod select;
mod insert;
pub mod parser;

use std::ops::Neg;
use std::path::{Path, PathBuf};
use comfy_table::{Table, TableComponent};
use csv::WriterBuilder;
use log::{info, warn};
use walkdir::WalkDir;
use crate::{csv_reader, Database};
use crate::sql::parser::OrderBy;

use crate::sql::parser::Statement::{Delete, Export, Import, Insert, Select, Label};

pub(crate) fn parse_and_run_sql(db: &mut Database, sql: String, auto_label_rules_file: &str) -> Result<(), String> {
    // First use our own parser to parse
    let result = parser::parse(&sql);

    match result {
        Ok((_input, statement)) => {
            match statement {
                Export(file_path) => {
                    execute_export_db(db, &file_path);
                }
                Import(account, file_path, inverse_amount, dryrun) => {
                    execute_import(db, &account, &file_path, inverse_amount, dryrun);
                }
                Select(projection, from, condition, order_by, limit, group_by) => {
                    select::run_select(db, projection, from, condition, order_by, limit, group_by, auto_label_rules_file);
                }
                Label(trans_ids, label_cmd) => {
                    for trans_id in trans_ids {
                        // TODO: avoid copying vec multiple times
                        db.apply_label_ops(trans_id, label_cmd.clone(), auto_label_rules_file)
                    }
                    info!("\nLabel operations completed.")
                }
                Insert(account, records) => {
                    let records_count = insert::execute_insert(db, account, records);
                    info!("\n{records_count} transactions inserted.");
                }
                Delete(trans_ids) => {
                    match trans_ids {
                        Some(trans_ids) => {
                            let trans_deleted = db.delete(&trans_ids);
                            info!("{trans_deleted} transactions deleted.");
                        },
                        None => info!("Unable to parse transaction IDs to delete, ignore operation.")
                    }
                }
            }
        },
        Err(e) => {
            return Err(e.to_string());
        }
    }

    info!("\n");

    Ok(())
}

extern crate dirs;

/// Import transactions from a file
pub(crate) fn execute_import(db : &mut Database, account :&str, file_path :&str, inverse_amount: bool, dry_run: bool) {
    let file_path: PathBuf = if file_path.starts_with("~/") {
        let mut path = dirs::home_dir().unwrap().into_os_string();
        path.push(&file_path[1..]);
        path.into()
    } else {
        PathBuf::from(file_path)
    };

    if file_path.is_dir() {
        info!("Parsing csv files from {} ...",file_path.display());
        for entry in WalkDir::new(file_path).into_iter() {
            let dir_entry = entry.unwrap();
            if dir_entry.path().is_file() && dir_entry.file_name().to_str().unwrap().to_lowercase().ends_with(".csv") {
                copy_from_csv(dir_entry.path(), db, account, inverse_amount, dry_run);
            }
        }
    } else if file_path.is_file() {
        copy_from_csv(file_path.as_path(), db, account, inverse_amount, dry_run);
    } else {
        warn!("{:?} does not exist", file_path);
    }
}

fn copy_from_csv(path: &Path, db: &mut Database, table_name: &str, mut inverse_amount: bool, dry_run: bool) {
    if dry_run {
        info!("Dry run. Printing transactions from {}", path.display());
    } else {
        info!("Importing transactions from {}", path.display());
    }

    let result = csv_reader::read_transactions(table_name, path);
    match result {
        Ok(mut records) => {
            if dry_run {
                let mut table = Table::new();
                table.set_header(vec!["Account", "Date", "Description", "Amount"]);
                table.remove_style(TableComponent::HorizontalLines);
                table.remove_style(TableComponent::MiddleIntersections);
                table.remove_style(TableComponent::LeftBorderIntersections);
                table.remove_style(TableComponent::RightBorderIntersections);
                for r in &records {
                    table.add_row(vec![r.account.as_str(), r.date.to_string().as_str(), r.description.as_str(), format!("{:.2}", r.amount).as_str()]);
                }
                println!("{table}");
                info!("This is a dry-run. Transactions are not imported");
                return;
            }

            // If inverse_amount flag is not set
            if !inverse_amount {
                // We should check if most transactions have positive amount. If this is the case it's likely to be
                // inverse amount, so we should prompt user

                let mut positive_amount_count = 0usize;
                for r in records.iter() {
                    if r.amount > 0.0 {
                        positive_amount_count += 1;
                    }
                }
                // If more than 50% of records have positive amount
                if positive_amount_count as f32 / records.len() as f32 > 0.5 {
                    // ask user if they want to set 'inverse_amount' flag to true
                    println!("Most transactions in {} have positive amount value.\n\
                    Do you want to set 'inverse_amount' flag so positive amount are treated as spending and \
                    negative are treated as income?\n\
                    yes or no, default is 'yes': ", path.display());

                    let mut user_input = String::new();
                    std::io::stdin().read_line(&mut user_input).unwrap();
                    let user_input = user_input.trim().to_lowercase();
                    if user_input.is_empty() || user_input == "yes" {
                        inverse_amount = true;
                    }
                }

                if inverse_amount {
                    for r in records.iter_mut() {
                        r.amount = r.amount.neg();
                    }
                }

                for r in &records {
                    db.upsert(r);
                }
                db.save();
                println!("Imported {} transactions", &records.len());
            }
        },
        Err(e) => {
            println!("{e}");
        }
    }
}

/// Export transactions to a file
pub(crate) fn execute_export_db(db : &mut Database, file_path :&str) {
    let transactions = db.query(None, None, OrderBy::date(), None);
    let mut csv_writer = WriterBuilder::new().has_headers(true).from_path(file_path).unwrap();
    for t in transactions {
        csv_writer.serialize(t).unwrap();
    }
    csv_writer.flush().unwrap();
}
