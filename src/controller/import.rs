use std::collections::BTreeSet;
use std::fs;
use std::ops::Neg;
use std::path::{Path, PathBuf};
use anyhow::anyhow;
use comfy_table::{Table, TableComponent};
use log::{info, warn};
use walkdir::{DirEntry, WalkDir};
use crate::csv_reader;
use crate::db::Database;

/// Import transactions from a file
pub(crate) fn execute_import(db : &mut Database, import_root_dir :&PathBuf, inverse_amount: bool, dry_run: bool) {
    let current_dir_files = scan_files(import_root_dir).unwrap();
    let new_files = diff_files(&db, &current_dir_files);
    if new_files.is_empty() {
        info!("No new statement files detected.");
        return;
    }

    for f in new_files.iter() {
        // Derive account name from the first segment of path.
        // E.g. for amex/2023-01.csv the account name will be 'amex'.
        let account = match f.split_once(std::path::MAIN_SEPARATOR) {
            None => "default",
            Some((first_segment, _)) => first_segment
        };

        let path = PathBuf::from(import_root_dir).join(f);
        let result = copy_from_csv(path.as_path(), db, account, inverse_amount, dry_run);
        match result {
            Ok(()) => {
                if !dry_run {
                    let md5 = md5::compute(fs::read(path).unwrap());
                    db.record_file_md5(f, md5).expect("Unable to record file md5");
                }
            },
            Err(e) => {
                warn!("{}", e)
            }
        }
    }
    db.save();
}

fn copy_from_csv(path: &Path, db: &mut Database, table_name: &str, mut inverse_amount: bool, dry_run: bool) -> anyhow::Result<()> {
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
                return Ok(());
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
            Ok(())
        },
        Err(e) => {
            Err(anyhow!(e))
        }
    }
}

/// Scan a dir recursively and list all eligible bank statement files
pub(crate) fn scan_files(root_path: &PathBuf) -> anyhow::Result<BTreeSet<String>> {
    info!("Scanning files in {}", root_path.to_str().unwrap());
    
    let mut files = BTreeSet::new();
    let walker = WalkDir::new(root_path).into_iter();
    for entry in walker.filter_entry(|e| !is_hidden(e)) {
        if let Ok(dir_entry) = entry {
            // Ignore symlinks
            if dir_entry.path_is_symlink() {
                continue;
            }

            let path = dir_entry.path();
            // Ignore directory
            if path.is_dir() {
                continue;
            }

            let canonical = path.canonicalize()?;
            // file_id is the sub path from the importing root dir.
            // E.g. if importing from /Users/ren/bank-statements, the file /Users/ren/bank-statements/amex/2023-01.csv
            // will have the file id 'amex/2023-01.csv'
            let file_id = canonical.strip_prefix(&root_path)?.to_str().unwrap();
            if file_id.ends_with(".csv") {
                files.insert(file_id.into());
            }
        }
    }

    Ok(files)
}

/// Return a list of files that's in the new list but not in the current list
pub(crate) fn diff_files(db: &Database, new: &BTreeSet<String>) -> BTreeSet<String> {
    let mut diff = BTreeSet::new();
    for f in new {
        if !db.file_exist(f) {
            diff.insert(f.into());
        }
    }

    diff
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}
