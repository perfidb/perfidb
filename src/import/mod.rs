use std::collections::BTreeSet;
use std::path::PathBuf;
use log::info;
use walkdir::{DirEntry, WalkDir};
use crate::db::Database;

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
