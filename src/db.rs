use std::{fs};
use std::collections::HashMap;
use std::path::Path;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use crate::transaction::Transaction;

/// Internal representation of a transaction record in database
#[derive(Serialize, Deserialize, Debug)]
struct TransactionRecord {
    account: String,
    date: NaiveDateTime,
    description: String,
    amount: f32,
    /// List of tag ids
    tags: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Database {
    transactions: Vec<TransactionRecord>,

    /// key is tag string, value is tag's index
    tags: HashMap<String, usize>,

    #[serde(skip_serializing)]
    file_path: Option<String>,
}

impl Database {
    pub(crate) fn new(file_path: Option<String>) -> Database {
        Database {
            transactions: vec![],
            tags: HashMap::new(),
            file_path,
        }
    }

    pub(crate) fn load(path_str: &str) -> Database {
        let path = Path::new(path_str);
        if path.exists() {
            let mut database :Database = serde_json::from_str(fs::read_to_string(path).unwrap().as_str()).unwrap();
            database.file_path = Some(path_str.to_string());
            database
        } else {
            Database::new(Some(path_str.to_string()))
        }
    }

    pub(crate) fn save_and_close(&self) {
        let s = serde_json::to_string(self).unwrap();
        fs::write((&self.file_path).as_ref().unwrap(), s).expect("Unable to write to database file");
    }

    pub(crate) fn upsert(&mut self, t: &Transaction) {
        let mut tags = vec![];
        for tag in t.tags.iter() {
            let tag_id = match self.tags.get(tag.as_str()) {
                Some(tag_index) => {
                    *tag_index
                },
                None => {
                    let tag_index = self.tags.len() + 1;
                    self.tags.insert(tag.into(), tag_index);
                    tag_index
                }
            };

            tags.push(tag_id);
        }

        self.transactions.push(TransactionRecord {
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            tags
        });
    }
}

