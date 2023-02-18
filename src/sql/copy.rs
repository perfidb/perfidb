use std::path::Path;
use csv::{WriterBuilder};
use log::{info, warn};
use sqlparser::ast::{CopyOption, CopyTarget};
use walkdir::WalkDir;
use crate::Database;
use crate::sql::copy_from_csv;

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
