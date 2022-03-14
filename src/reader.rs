use std::io;
use chrono::NaiveDateTime;
use serde::Deserialize;
use crate::transaction::Transaction;

#[derive(Deserialize, Debug)]
pub(crate) struct CsvRow {
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    /// tags must be a comma delimited string
    pub(crate) tags: String,
}

pub(crate) fn read_transactions<R: io::Read>(reader: R) -> Vec<Transaction> {
    let mut transactions :Vec<Transaction> = vec![];
    let mut rdr = csv::Reader::from_reader(reader);
    for result in rdr.deserialize() {
        let row: CsvRow = result.unwrap();
        transactions.push(row.into());
    }

    transactions
}