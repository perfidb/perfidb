use std::path::PathBuf;
use crate::csv_reader::{detect_header_row, read_transactions};

#[test]
fn test_detect_header_row() {
    let result = detect_header_row(&fixture_filename("header.csv"));
    match result {
        Some(header_row) => {
            assert_eq!(header_row.get(0), Some("Time"));
            assert_eq!(header_row.get(1), Some("BSB / Account Number"));
        },
        None => panic!("Unexpected results")
    }
}

#[test]
fn test_read_transactions() {
    let results = read_transactions("amex", &fixture_filename("header.csv"), false);
    match results {
        Ok(rows) => {
            assert_eq!(rows.len(), 4);
        },
        Err(e) => panic!("Unexpected results")
    }
}

/// Return the path to a file within the test data directory
pub(crate) fn fixture_filename(filename: &str) -> PathBuf {
    let mut dir = fixture_dir();
    dir.push(filename);
    dir
}

pub(crate) fn fixture_dir() -> PathBuf {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    dir.push("fixture");
    dir
}
