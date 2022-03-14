use std::collections::HashSet;
use chrono::NaiveDateTime;
use crate::reader::CsvRow;

#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    pub(crate) tags: HashSet<String>,
}

impl From<CsvRow> for Transaction {
    fn from(item: CsvRow) -> Self {
        Transaction {
            account: item.account,
            date: item.date,
            description: item.description,
            amount: item.amount,
            tags: item.tags.split(",").map(|s| s.to_string()).collect()
        }
    }
}