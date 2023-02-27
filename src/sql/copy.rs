use std::path::Path;
use comfy_table::{Table, TableComponent};
use csv::{WriterBuilder};
use log::{info, warn};
use sqlparser::ast::{CopyOption, CopyTarget};
use walkdir::WalkDir;
use crate::{csv_reader, Database};

/// Import transactions from a file
pub(crate) fn execute_import(db : &mut Database, table_name :&str, target: &CopyTarget, options: &[CopyOption]) {
    // should we inverse amount value
    let mut inverse_amount = false;
    let mut dry_run = false;
    for option in options {
        if let CopyOption::Format(ident) = option {
            let format_value = ident.value.to_lowercase();
            if format_value == "i" || format_value == "inverse" {
                inverse_amount = true;
            } else if format_value == "dryrun" {
                dry_run = true;
            }
        }
    }

    match target {
        CopyTarget::File { filename} => {
            let path = Path::new(filename);
            if path.is_dir() {
                for entry in WalkDir::new(path).into_iter() {
                    let dir_entry = entry.unwrap();
                    if dir_entry.path().is_file() && !dir_entry.file_name().to_str().unwrap().starts_with('.') {
                        info!("Copying from {}", dir_entry.path().display());
                        copy_from_csv(dir_entry.path(), db, table_name, inverse_amount, dry_run);
                    }
                }
            } else if path.is_file() {
                info!("Copying from {}", path.display());
                copy_from_csv(path, db, table_name, inverse_amount, dry_run);
            }
        },
        _ => {
            warn!("Import from non file source is not supported yet. Source: {target:?}");
        }
    }
}

fn copy_from_csv(path: &Path, db: &mut Database, table_name: &str, inverse_amount: bool, dry_run: bool) {
    let result = csv_reader::read_transactions(table_name, path, inverse_amount);
    match result {
        Ok(records) => {
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
            } else {
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
pub(crate) fn execute_export(db : &mut Database, table_name :&str, target: &CopyTarget) {
    let transactions = db.query(table_name, None);
    if let CopyTarget::File { filename } = target {
        let mut csv_writer = WriterBuilder::new().has_headers(true).from_path(filename).unwrap();
        for t in transactions {
            csv_writer.serialize(t).unwrap();
        }
        csv_writer.flush().unwrap();
    }
}
