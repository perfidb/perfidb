use std::clone;
use std::slice::Iter;
use chrono::NaiveDateTime;
use serde::Deserialize;

pub struct Database {
    transactions: Vec<Transaction>
}

impl Database {
    pub(crate) fn new() -> Database {
        Database {
            transactions: vec![],
        }
    }

    pub(crate) fn upsert(&mut self, t: Transaction) {
        self.transactions.push(t);
    }

    pub(crate) fn iter(&self) -> Iter<'_, Transaction> {
        self.transactions.iter()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Transaction {
    date: NaiveDateTime,
    description: String,
    amount: f32,
}