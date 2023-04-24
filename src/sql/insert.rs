use log::info;
use crate::csv_reader::Record;
use crate::Database;

/// Execute an INSERT statement
pub(crate) fn execute_insert(db : &mut Database, account: Option<String>, records: Vec<Record>) {
    let account_name = match account {
        Some(account_name) => account_name,
        None => "default".to_string()
    };

    let mut total_inserted = 0;
    for record in records {
        let mut new_record = record.clone();
        new_record.account = account_name.clone();
        db.upsert(&new_record);
        total_inserted += 1;
    }

    db.save();

    info!("{total_inserted} transactions inserted");
}
