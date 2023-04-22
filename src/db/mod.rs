mod filter;
mod search;
mod minhash;

use std::fs;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};
use log::info;
use rustyline::ConditionalEventHandler;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::ast::Expr::Identifier;
use crate::common::ResultError;

use crate::csv_reader::Record;
use minhash::StringMinHash;
use sql::parser::Condition;
use crate::db::search::SearchIndex;
use crate::sql;
use crate::sql::parser::{Operator, Projection};
use crate::tagger::Tagger;
use crate::transaction::Transaction;

/// perfidb binary version
const PERFIDB_VERSION: &str = env!("CARGO_PKG_VERSION");

const ALL_ACCOUNTS: &str = "db";

/// Internal representation of a transaction record in database
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TransactionRecord {
    id: u32,
    account: String,
    date: NaiveDateTime,
    description: String,
    amount: f32,

    // List of tag ids
    labels: Vec<u32>,
}

impl TransactionRecord {
    pub(crate) fn has_tags(&self) -> bool {
        !self.labels.is_empty()
    }
}

/// Metadata of database file. Contains the version of perfidb that was used to write the database to disk.
/// Will be used by future version of perfidb to upgrade database file written by older version of binary.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Metadata {
    version: String
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Database {
    transaction_id_seed: u32,
    transactions: HashMap<u32, TransactionRecord>,

    /// Key is transaction date, value is a list of transaction ids.
    date_index: BTreeMap<NaiveDate, Vec<u32>>,

    label_minhash: StringMinHash,

    /// label id to a list of transactions with that tag
    label_id_to_transactions: HashMap<u32, Vec<u32>>,

    /// Inverted index for full-text search on 'description'
    search_index: SearchIndex,

    #[serde(skip_serializing, skip_deserializing)]
    file_path: Option<String>,

    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) last_query_results: Option<Vec<u32>>,
}

impl Database {
    pub(crate) fn new(file_path: String) -> Database {
        Database {
            transaction_id_seed: 1,
            transactions: HashMap::new(),
            date_index: BTreeMap::new(),
            label_minhash: StringMinHash::new(),
            label_id_to_transactions: HashMap::new(),
            search_index: SearchIndex::new(),
            file_path: Some(file_path),
            last_query_results: None,
        }
    }

    pub(crate) fn load(path_str: &str) -> ResultError<Database> {
        let path = Path::new(path_str);
        if path.exists() {
            let mut file = fs::File::open(path)?;
            let metadata_len = file.read_u16::<LittleEndian>()?;
            let mut buffer = vec![0; metadata_len as usize];
            file.read_exact(&mut buffer)?;
            let metadata: Metadata = bincode::deserialize(&buffer)?;
            info!("Database version {}", metadata.version);

            file.seek(SeekFrom::Start(1024))?;
            let mut buffer: Vec<u8> = vec![];
            file.read_to_end(&mut buffer)?;

            let mut database :Database = bincode::deserialize(&buffer)?;
            database.file_path = Some(path_str.to_string());
            Ok(database)
        } else {
            Ok(Database::new(path_str.to_string()))
        }
    }

    /// Save db content to disk
    pub(crate) fn save(&self) {
        // Create metadata using current binary version
        let metadata = Metadata { version: PERFIDB_VERSION.to_string() };
        let metadata_encoded: Vec<u8> = bincode::serialize(&metadata).unwrap();
        let metadata_length = metadata_encoded.len();
        assert!(metadata_length <= (u16::MAX - 2) as usize);

        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();

        // Use first 1024 bytes to store metadata
        let mut file = fs::File::create(self.file_path.as_ref().unwrap()).unwrap();
        // Using first 2 bytes to write metadata length
        file.write_u16::<LittleEndian>(metadata_length as u16).unwrap();
        // Write metadata
        file.write_all(&metadata_encoded).unwrap();
        let remaining_header_bytes = 1024 - 2 - metadata_length;
        // Write 0s for remaining bytes to fill up the first 1024 bytes.
        file.write_all(&vec![0; remaining_header_bytes]).unwrap();

        file.write_all(&encoded).expect("Unable to write to database file");
        file.flush().unwrap();
    }

    pub(crate) fn upsert(&mut self, t: &Record) {
        let trans_id = match t.id {
            Some(id) => id,
            None => self.transaction_id_seed
        };

        if trans_id == self.transaction_id_seed {
            self.transaction_id_seed += 1;
        } else if trans_id > self.transaction_id_seed {
            self.transaction_id_seed = trans_id + 1;
        }

        let labels = match &t.labels {
            Some(l) => l.clone(),
            None => vec![]
        }.iter().map(|l| self.label_minhash.put(l)).collect();

        let t = TransactionRecord {
            id: trans_id,
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            labels,
        };

        let date: NaiveDate = t.date.date();
        self.date_index.entry(date).or_insert(vec![]);

        // Add to date index
        self.date_index.get_mut(&date).unwrap().push(trans_id);

        self.search_index.index(&t);

        // Add to transactions table
        self.transactions.insert(trans_id, t);
    }

    pub(crate) fn update_labels(&mut self, trans_id: u32, labels: &str) {
        let labels: Vec<&str> = labels.split(',').map(|t| t.trim()).filter(|t| !t.is_empty()).collect();

        let mut existing_labels = HashSet::<String>::new();
        for tag_id in self.transactions.get(&trans_id).unwrap().labels.iter() {
            existing_labels.insert(self.label_minhash.lookup_by_hash(tag_id).unwrap().clone());
        }

        let mut tags_to_remove :Vec<&str> = vec![];
        let mut tags_to_add :Vec<&str> = vec![];
        for existing_label in existing_labels.iter() {
            if !labels.contains(&existing_label.as_str()) {
                tags_to_remove.push(existing_label);
            }
        }
        for new_label in labels {
            if !existing_labels.contains(new_label) {
                tags_to_add.push(new_label);
            }
        }

        if !tags_to_remove.is_empty() {
            self.remove_tags(trans_id, &tags_to_remove);
        }

        if !tags_to_add.is_empty() {
            self.add_labels(trans_id, &tags_to_add);
        }
    }

    pub(crate) fn add_labels(&mut self, trans_id: u32, labels_to_add: &[&str]) {
        info!("Adding labels {:?} for transaction {}", labels_to_add, trans_id);

        for label in labels_to_add {
            // Ensure label is in minhash. Get the minhash for this label.
            let label_id = self.label_minhash.put(label.to_string());
            let transaction = self.transactions.get_mut(&trans_id).unwrap();
            if !transaction.labels.contains(&label_id) {
                transaction.labels.push(label_id);
                // self.label_id_to_transactions may not have the new label_id
                self.label_id_to_transactions.entry(label_id).or_insert(vec![]).push(transaction.id);
            }
        }

        self.save();
    }

    pub(crate) fn set_labels_for_multiple_transactions(&mut self, where_clause: &Expr, labels: &[&str]) {
        let mut transactions = HashSet::<u32>::new();
        for trans_id in self.transactions.keys() {
            transactions.insert(*trans_id);
        }

        transactions = self.filter_transactions(&transactions, where_clause);

        for label in labels {
            let label_id = self.label_minhash.put(label.to_string());

            for trans_id in &transactions {
                let transaction = self.transactions.get_mut(trans_id).unwrap();
                if !transaction.labels.contains(&label_id) {
                    transaction.labels.push(label_id);
                    self.label_id_to_transactions.entry(label_id).or_insert(vec![]).push(transaction.id);
                }
            }
        }

        self.save();
    }

    pub(crate) fn auto_label(&mut self, auto_labeller: &Tagger, where_clause: &Expr) {
        let mut transactions = HashSet::<u32>::new();
        for trans_id in self.transactions.keys() {
            transactions.insert(*trans_id);
        }

        transactions = self.filter_transactions(&transactions, where_clause);
        for trans_id in &transactions {
            let t = self.transactions.get(trans_id).unwrap();
            let labels = auto_labeller.label(&self.to_transaction(t));
            if !labels.is_empty() {
                self.add_labels(*trans_id, &labels.iter().map(|s| s.as_str()).collect::<Vec<&str>>());
            }
        }
    }

    pub(crate) fn remove_tags(&mut self, trans_id: u32, labels: &[&str]) {
        info!("Removing tags {:?} from transaction {}", labels, trans_id);
        let transaction = self.transactions.get_mut(&trans_id).unwrap();

        for label in labels {
            // Only run if this tag id exists
            if let Some(label_id_to_remove) = self.label_minhash.lookup_by_string(*label) {
                transaction.labels.retain(|existing_id| *existing_id != label_id_to_remove);
                // Remove transaction from dictionary
                self.label_id_to_transactions.get_mut(&label_id_to_remove).unwrap().retain(|existing_trans_id| *existing_trans_id != trans_id);
            }
        }

        self.save();
    }

    /// Filter transactions based on the given SQL where clause.
    /// Returns the set of transaction ids after applying the filter.
    fn filter_transactions(&self, transactions: &HashSet<u32>, where_clause: &Expr) -> HashSet<u32> {
        info!("{:?}", where_clause);

        match where_clause {
            Expr::BinaryOp{ left, op: BinaryOperator::Eq, right } => {
                let left: &Expr = left;
                let right: &Expr = right;

                filter::handle_equals((*left).clone(), (*right).clone(), self, transactions)
            },

            Expr::BinaryOp { left, op: BinaryOperator::NotEq, right } => {
                let left: &Expr = left;
                let right: &Expr = right;

                filter::handle_not_equal((*left).clone(), (*right).clone(), self, transactions)
            },

            // If it is 'LIKE' operator, we assume it's  description LIKE '...', so we don't check left
            Expr::Like { pattern, ..} => {
                filter::handle_like((**pattern).clone(), self)
            },

            // label IS NULL
            Expr::IsNull(expr) => {
                // Had to unbox here. Rust 1.63
                let expr :&Expr = expr;
                if let Identifier(ident) = expr {
                    if ident.value == "label" {
                        return transactions.iter().filter(|id| !self.transactions.get(id).unwrap().has_tags()).cloned().collect::<HashSet<u32>>();
                    }
                }
                HashSet::new()
            },

            // label IS NOT NULL
            Expr::IsNotNull(expr) => {
                // Had to unbox here. Rust 1.63
                let expr :&Expr = expr;
                if let Identifier(ident) = expr {
                    if ident.value == "label" {
                        return transactions.iter().filter(|id| self.transactions.get(id).unwrap().has_tags()).cloned().collect::<HashSet<u32>>();
                    }
                }
                HashSet::new()
            },

            // Process left > right, assumes left is 'amount'
            Expr::BinaryOp{ left: _, op: BinaryOperator::Gt, right} => {
                // TODO: handle UnaryOp {op: Minus, expr: Value(Number("11.99", false))} properly
                let s: String = right.to_string().replace(' ', "");
                let amount_limit = s.parse::<f32>().unwrap();

                transactions.iter().filter(|id| self.transactions.get(id).unwrap().amount > amount_limit).cloned().collect::<HashSet<u32>>()
            },
            Expr::BinaryOp{ left: _, op: BinaryOperator::GtEq, right} => {
                let s: String = right.to_string().replace(' ', "");
                let amount_limit = s.parse::<f32>().unwrap();

                transactions.iter().filter(|id| self.transactions.get(id).unwrap().amount >= amount_limit).cloned().collect::<HashSet<u32>>()
            },


            // Process left < right, assumes left is 'amount'
            Expr::BinaryOp{ left: _, op: BinaryOperator::Lt, right} => {
                let s: String = right.to_string().replace(' ', "");
                let amount_limit = s.parse::<f32>().unwrap();

                transactions.iter().filter(|id| self.transactions.get(id).unwrap().amount < amount_limit).cloned().collect::<HashSet<u32>>()
            },
            Expr::BinaryOp{ left: _, op: BinaryOperator::LtEq, right} => {
                let s: String = right.to_string().replace(' ', "");
                let amount_limit = s.parse::<f32>().unwrap();

                transactions.iter().filter(|id| self.transactions.get(id).unwrap().amount <= amount_limit).cloned().collect::<HashSet<u32>>()
            },

            Expr::BinaryOp{ left, op: BinaryOperator::And, right} => {
                let left_result = self.filter_transactions(transactions, left);
                let right_result = self.filter_transactions(transactions, right);
                left_result.intersection(&right_result).cloned().collect()
            },

            Expr::BinaryOp{ left, op: BinaryOperator::Or, right} => {
                let left_result = self.filter_transactions(transactions, left);
                let right_result = self.filter_transactions(transactions, right);
                left_result.union(&right_result).cloned().collect()
            },

            Expr::Nested(n) => {
                self.filter_transactions(transactions, n)
            },

            _ => HashSet::new()
        }
    }

    /// Filter transactions based on the given SQL where clause.
    /// Returns the set of transaction ids after applying the filter.
    fn filter_transactions_new(&self, transactions: &HashSet<u32>, condition: Condition) -> HashSet<u32> {
        let get_amount = |id| self.transactions.get(id).unwrap().amount;

        match condition {
            Condition::Spending(op, spending) => {
                let amount_limit = -spending;
                match op {
                    Operator::Gt => transactions.iter().filter(|id| get_amount(id) < amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::GtEq => transactions.iter().filter(|id| get_amount(id) <= amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::Lt => transactions.iter().filter(|id| {
                        let amount = get_amount(id);
                        amount > amount_limit && amount <= 0.0
                    }).cloned().collect::<HashSet<u32>>(),
                    Operator::LtEq => transactions.iter().filter(|id| {
                        let amount = get_amount(id);
                        amount >= amount_limit && amount <= 0.0
                    }).cloned().collect::<HashSet<u32>>(),
                    Operator::Eq => transactions.iter().filter(|id| get_amount(id) == amount_limit).cloned().collect::<HashSet<u32>>(),
                    _ => HashSet::new(),
                }
            }

            Condition::Income(op, income_limit) => {
                match op {
                    Operator::Gt => transactions.iter().filter(|id| get_amount(id) > income_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::GtEq => transactions.iter().filter(|id| get_amount(id) >= income_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::Lt => transactions.iter().filter(|id| {
                        let amount = get_amount(id);
                        amount >= 0.0 && amount < income_limit
                    }).cloned().collect::<HashSet<u32>>(),
                    Operator::LtEq => transactions.iter().filter(|id| {
                        let amount = get_amount(id);
                        amount >= 0.0 && amount <= income_limit
                    }).cloned().collect::<HashSet<u32>>(),
                    Operator::Eq => transactions.iter().filter(|id| get_amount(id) == income_limit).cloned().collect::<HashSet<u32>>(),
                    _ => HashSet::new(),
                }
            }

            Condition::Amount(op, amount_limit) => {
                match op {
                    Operator::Gt => transactions.iter().filter(|id| get_amount(id) > amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::GtEq => transactions.iter().filter(|id| get_amount(id) >= amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::Lt => transactions.iter().filter(|id| get_amount(id) < amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::LtEq => transactions.iter().filter(|id| get_amount(id) <= amount_limit).cloned().collect::<HashSet<u32>>(),
                    Operator::Eq => transactions.iter().filter(|id| get_amount(id) == amount_limit).cloned().collect::<HashSet<u32>>(),
                    _ => HashSet::new(),
                }
            }

            // Assuming op is 'Match' for now
            Condition::Description(_op, keyword) => {
                self.search_index.search(&keyword)
            }

            // Assuming op is '='
            Condition::Label(_op, label) => {
                match self.label_minhash.lookup_by_string(label) {
                    Some(label_id) => transactions.iter().filter(|id| self.transactions.get(id).unwrap().labels.contains(&label_id)).cloned().collect::<HashSet<u32>>(),
                    None => HashSet::new()
                }
            }

            Condition::Date(_op, date_range) => {
                let mut trans_in_date_range = HashSet::<u32>::new();
                for (_, trans_ids) in self.date_index.range(date_range) {
                    for id in trans_ids {
                        trans_in_date_range.insert(*id);
                    }
                }
                trans_in_date_range
            }

            Condition::And(sub_conditions) => {
                let c1_result = self.filter_transactions_new(transactions, (*sub_conditions).0);
                let c2_result = self.filter_transactions_new(transactions, (*sub_conditions).1);
                c1_result.intersection(&c2_result).cloned().collect()
            }

            Condition::Or(sub_conditions) => {
                let c1_result = self.filter_transactions_new(transactions, (*sub_conditions).0);
                let c2_result = self.filter_transactions_new(transactions, (*sub_conditions).1);
                c1_result.union(&c2_result).cloned().collect()
            }
        }
    }

    /// The new query implementation
    pub(crate) fn query_new(&mut self, from: Option<String>, condition: Option<Condition>) -> Vec<Transaction> {
        let mut trans :HashSet<u32> = match from {
            None => self.transactions.keys().cloned().collect::<HashSet<u32>>(),
            Some(account) => self.transactions.values().filter(|t| account == t.account).map(|t| t.id).collect()
        };

        if let Some(condition) = condition {
            trans = self.filter_transactions_new(&trans, condition);
        }

        let mut trans :Vec<&TransactionRecord> = trans.iter().map(|id| self.transactions.get(id).unwrap()).collect();
        trans.sort_by(|a, b| {
            a.date.partial_cmp(&b.date).unwrap().then(a.id.partial_cmp(&b.id).unwrap())
        });

        let results :Vec<Transaction> = trans.iter().map(|t| self.to_transaction(t)).collect();
        if !results.is_empty() {
            self.last_query_results = Some(results.iter().map(|t|t.id).collect());
        }

        results
    }




    /// Current implementation is quite bad. Hope we can use a better way to do this in Rust
    pub(crate) fn query(&mut self, account: &str, where_clause: Option<Expr>) -> Vec<Transaction> {
        let mut transactions = if account.to_ascii_lowercase() == ALL_ACCOUNTS {
            self.transactions.keys().cloned().collect::<HashSet<u32>>()
        } else {
            self.transactions.values().filter(|t| account == t.account).map(|t| t.id).collect::<HashSet<u32>>()
        };

        if let Some(where_clause) = where_clause {
            transactions = self.filter_transactions(&transactions, &where_clause);
        }

        let mut transactions = transactions.iter().map(|id| self.transactions.get(id).unwrap()).collect::<Vec<&TransactionRecord>>();

        transactions.sort_by(|a, b| {
            a.date.partial_cmp(&b.date).unwrap().then(a.id.partial_cmp(&b.id).unwrap())
        });

        let results :Vec<Transaction> = transactions.iter().map(|t| self.to_transaction(t)).collect();

        if !results.is_empty() {
            self.last_query_results = Some(results.iter().map(|t|t.id).collect());
        }

        results
    }

    pub(crate) fn find_by_id(&self, id: u32) -> Transaction {
        let t = self.transactions.get(&id).unwrap();
        self.to_transaction(t)
    }

    pub(crate) fn search_by_id(&self, id: u32) -> Option<Transaction> {
        self.transactions.get(&id).map(|t| self.to_transaction(t))
    }

    fn to_transaction(&self, t: &TransactionRecord) -> Transaction {
        // TODO: use a function to format tags
        Transaction::new(t.id, t.account.clone(), t.date, t.description.as_str(), t.amount,
                         t.labels.iter().map(|tag_id| self.label_minhash.lookup_by_hash(tag_id).unwrap().clone()).collect::<Vec<String>>())
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_transaction_serde() {
        let t = TransactionRecord {
            id: 1,
            account: "cba".to_string(),
            date: NaiveDateTime::from_str("2022-07-31T17:30:45").unwrap(),
            description: "food".to_string(),
            amount: 29.95,
            labels: vec![]
        };

        let s = serde_json::to_string::<TransactionRecord>(&t).unwrap();
        println!("{}", s);
    }
}
