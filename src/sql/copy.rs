use std::path::Path;
use comfy_table::{Table, TableComponent};
use csv::{WriterBuilder};
use log::{info};
use walkdir::WalkDir;
use crate::{csv_reader, Database};

/// Import transactions from a file
pub(crate) fn execute_import(db : &mut Database, account :&str, file_path :&str, inverse_amount: bool, dry_run: bool) {
    let file_path = Path::new(file_path);
    if file_path.is_dir() {
        for entry in WalkDir::new(file_path).into_iter() {
            let dir_entry = entry.unwrap();
            if dir_entry.path().is_file() && !dir_entry.file_name().to_str().unwrap().starts_with('.') {
                info!("Copying from {}", dir_entry.path().display());
                copy_from_csv(dir_entry.path(), db, account, inverse_amount, dry_run);
            }
        }
    } else if file_path.is_file() {
        info!("Copying from {}", file_path.display());
        copy_from_csv(file_path, db, account, inverse_amount, dry_run);
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
pub(crate) fn execute_export_db(db : &mut Database, file_path :&str) {
    let transactions = db.query("db", None);
    let mut csv_writer = WriterBuilder::new().has_headers(true).from_path(file_path).unwrap();
    for t in transactions {
        csv_writer.serialize(t).unwrap();
    }
    csv_writer.flush().unwrap();
}
