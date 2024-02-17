use csv::WriterBuilder;
use crate::db::Database;
use crate::parser::OrderBy;

/// Export transactions to a file
pub(crate) fn execute_export_db(db : &mut Database, file_path :&str) {
    let transactions = db.query(None, None, OrderBy::date(), None);
    let mut csv_writer = WriterBuilder::new().has_headers(true).from_path(file_path).unwrap();
    for t in transactions {
        csv_writer.serialize(t).unwrap();
    }
    csv_writer.flush().unwrap();
}