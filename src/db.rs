use std::{fmt, fs};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path};
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr};
use crate::transaction::Transaction;
use crate::enrich::enrich;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum TransactionKind {
    Income,
    Expense,
    LargeExpense,
    Transfer
}

impl fmt::Display for TransactionKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TransactionKind::Income => write!(f, "Income"),
            TransactionKind::Expense => write!(f, "Expense"),
            TransactionKind::LargeExpense => write!(f, "LargeExpense"),
            TransactionKind::Transfer => write!(f, "Transfer"),
        }
    }
}

/// Internal representation of a transaction record in database
#[derive(Serialize, Deserialize, Debug)]
struct TransactionRecord {
    id: u32,
    account: String,
    date: NaiveDateTime,
    description: String,
    amount: f32,

    // TODO: handle tags
    // List of tag ids
    // tags: Vec<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Database {
    transactions: Vec<TransactionRecord>,

    /// Key is transaction date, value is a list of transaction ids. The id is basically the index of 'transactions' vec.
    date_index: BTreeMap<NaiveDate, Vec<u32>>,

    /// key is tag string, value is tag's index
    tag_name_to_id: HashMap<String, usize>,

    tags: Vec<String>,

    #[serde(skip_serializing, skip_deserializing)]
    file_path: Option<String>,
}

impl Database {
    pub(crate) fn new(file_path: Option<String>) -> Database {
        Database {
            transactions: vec![],
            date_index: BTreeMap::new(),
            tag_name_to_id: HashMap::new(),
            tags: vec![],
            file_path,
        }
    }

    pub(crate) fn load(path_str: &str) -> Database {
        let path = Path::new(path_str);
        if path.exists() {
            let mut database :Database = bincode::deserialize(&fs::read(path).unwrap()).unwrap();
            // let mut database :Database = serde_json::from_str(fs::read_to_string(path).unwrap().as_str()).unwrap();
            database.file_path = Some(path_str.to_string());
            database
        } else {
            Database::new(Some(path_str.to_string()))
        }
    }

    pub(crate) fn save(&self) {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        // let s = serde_json::to_string(self).unwrap();
        fs::write((&self.file_path).as_ref().unwrap(), encoded).expect("Unable to write to database file");
    }

    pub(crate) fn upsert(&mut self, t: &Transaction) {
        // let mut tags = vec![];
        // for tag in t.tags.iter() {
        //     let tag_id = match self.tag_name_to_id.get(tag.as_str()) {
        //         Some(tag_id) => {
        //             *tag_id
        //         },
        //         None => {
        //             let tag_id = self.tags.len();
        //             self.tags.push(tag.into());
        //             self.tag_name_to_id.insert(tag.into(), tag_id);
        //             tag_id
        //         }
        //     };
        //
        //     tags.push(tag_id);
        // }

        let id = (self.transactions.len() + 1) as u32;
        let t = TransactionRecord {
            id,
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
        };

        let date: NaiveDate = t.date.date();
        if !self.date_index.contains_key(&date) {
            self.date_index.insert(date, vec![]);
        };
        // Add to date index
        self.date_index.get_mut(&date).unwrap().push(id);
        // Add to transactions table
        self.transactions.push(t);
    }

    /// Get transaction by id
    fn by_id(&self, id: u32) -> &TransactionRecord {
        &self.transactions[(id - 1) as usize]
    }

    /// Current implementation is quite bad. Hope we can use a better way to do this in Rust
    pub(crate) fn query(&self, account: &str, binary_op: Option<Expr>) -> Vec<Transaction> {
        let mut transactions = self.transactions.iter().filter(|t| {
            account == "all" || account == t.account
        }).collect::<Vec<&TransactionRecord>>();

        // TODO: half implemented 'amount > ...'
        if let Some(binary_op) = binary_op {
            if let Expr::BinaryOp { left: _, op, right } = binary_op {
                match op {
                    BinaryOperator::Gt => {
                        let s: String = right.to_string();
                        let amount_limit = s.parse::<f32>().unwrap();

                        transactions = transactions.into_iter().filter(|t| t.amount.abs() > amount_limit).collect::<Vec<&TransactionRecord>>();
                    },
                    _ => {}
                }
            }
        }

        transactions.iter().map(|t| Transaction {
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            kind: "".to_string(),
        }).collect()
    }
}


#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use super::*;

    #[test]
    fn test_transaction_serde() {
        let t = TransactionRecord {
            account: "cba".to_string(),
            date: NaiveDateTime::from_str("2022-07-31T17:30:45").unwrap(),
            description: "food".to_string(),
            amount: 29.95,
        };

        let s = serde_json::to_string::<TransactionRecord>(&t).unwrap();
        println!("{}", s);
    }
}
