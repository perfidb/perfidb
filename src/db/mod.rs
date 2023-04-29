mod search;
mod minhash;
mod roaring_bitmap;
mod label_id_vec;
pub(crate) mod label_op;

use std::fs;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Seek, SeekFrom, Write};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};
use log::{debug};
use serde::{Deserialize, Serialize};
use crate::common::ResultError;

use crate::csv_reader::Record;
use minhash::StringMinHash;
use sql::parser::Condition;
use crate::config::Config;
use crate::db::label_id_vec::LabelIdVec;
use crate::db::label_op::{LabelCommand, LabelOp};
use crate::db::roaring_bitmap::PerfidbRoaringBitmap;
use crate::db::search::SearchIndex;
use crate::sql;
use crate::sql::parser::{Operator};
use crate::labeller::Labeller;
use crate::transaction::Transaction;

/// perfidb binary version
const PERFIDB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Internal representation of a transaction record in database
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TransactionRecord {
    id: u32,
    account: String,
    date: NaiveDateTime,
    description: String,
    amount: f32,

    // List of label ids
    labels: LabelIdVec,
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
    date_index: BTreeMap<NaiveDate, PerfidbRoaringBitmap>,

    label_minhash: StringMinHash,

    /// label id to a list of transactions with that tag
    label_id_to_transactions: HashMap<u32, PerfidbRoaringBitmap>,

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

            debug!("Database metadata version {}", metadata.version);

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

        let date: NaiveDate = t.date.date();
        // Add to date index
        self.date_index.entry(date).or_insert(PerfidbRoaringBitmap::new()).insert(trans_id);

        let label_ids = match &t.labels {
            Some(labels) => {
                let label_ids: Vec<u32> = labels.iter().map(|l| self.label_minhash.put(l)).collect();
                LabelIdVec::from_vec(label_ids)
            },
            None => LabelIdVec::empty()
        };

        // Add to label index
        for label_id in &*label_ids {
            self.label_id_to_transactions.entry(*label_id).or_insert(PerfidbRoaringBitmap::new())
                .insert(trans_id);
        }

        let t = TransactionRecord {
            id: trans_id,
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            labels: label_ids,
        };
        self.search_index.index(&t);

        // Add to transactions table
        self.transactions.insert(trans_id, t);
    }

    /// Applying labelling operations on a transaction
    pub(crate) fn apply_label_ops(&mut self, trans_id: u32, label_cmd: LabelCommand, auto_label_rules_file: &str) {
        match label_cmd {
            LabelCommand::Manual(label_ops) => {
                for op in label_ops {
                    self.transactions.entry(trans_id).and_modify(|transaction| {
                        match op.op {
                            label_op::Operation::Add => {
                                let label_hash = self.label_minhash.put(op.label);
                                self.label_id_to_transactions.entry(label_hash).or_insert(PerfidbRoaringBitmap::new()).insert(trans_id);
                                // Add the label id to transaction
                                transaction.labels.add(label_hash);
                            },

                            label_op::Operation::Remove => {
                                if let Some(label_hash) = self.label_minhash.lookup_by_string(op.label) {
                                    self.label_id_to_transactions.entry(label_hash).and_modify(|bitmap| {
                                        bitmap.remove(trans_id);
                                    });
                                    // Remove labels from transaction
                                    transaction.labels.remove(label_hash);
                                }
                            }
                        }
                    });
                }
            }

            LabelCommand::Auto => {
                if let Some(transaction) = self.transactions.get(&trans_id) {
                    let mut label_ops: Vec<LabelOp> = vec![];
                    for label_hash in (*transaction.labels).iter() {
                        label_ops.push(LabelOp::new_remove(self.label_minhash.lookup_by_hash(label_hash).unwrap()));
                    }
                    let tagger = Labeller::new(&Config::load_from_file(auto_label_rules_file));
                    for new_label in tagger.label(&transaction.description) {
                        label_ops.push(LabelOp::new_add(&new_label));
                    }

                    self.apply_label_ops(trans_id, LabelCommand::Manual(label_ops), auto_label_rules_file);
                }
            }
        }
    }

    /// Filter transactions based on the given SQL where clause.
    /// Returns the set of transaction ids after applying the filter.
    fn filter_transactions(&self, transactions: &HashSet<u32>, condition: Condition) -> HashSet<u32> {
        let get_amount = |id| self.transactions.get(id).unwrap().amount;

        match condition {
            Condition::Id(id) => {
                let mut trans = HashSet::new();
                if self.search_by_id(id).is_some() {
                    trans.insert(id);
                }
                trans
            }

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

            Condition::Label(op, label) => {
                match op {
                    Operator::Eq => {
                        match self.label_minhash.lookup_by_string(label) {
                            Some(label_id) => self.label_id_to_transactions.get(&label_id).unwrap().iter().collect::<HashSet<u32>>(),
                            None => HashSet::new()
                        }
                    }

                    Operator::IsNull => {
                        transactions.iter().filter(|id| !self.transactions.get(id).unwrap().has_tags()).cloned().collect::<HashSet<u32>>()
                    }

                    Operator::IsNotNull => {
                        transactions.iter().filter(|id| self.transactions.get(id).unwrap().has_tags()).cloned().collect::<HashSet<u32>>()
                    }

                    _ => HashSet::new()
                }
            }

            Condition::Date(_op, date_range) => {
                let mut trans_in_date_range = HashSet::<u32>::new();
                for (_, trans_ids) in self.date_index.range(date_range) {
                    for id in trans_ids.iter() {
                        trans_in_date_range.insert(id);
                    }
                }
                trans_in_date_range
            }

            Condition::And(sub_conditions) => {
                let c1_result = self.filter_transactions(transactions, (*sub_conditions).0);
                let c2_result = self.filter_transactions(transactions, (*sub_conditions).1);
                c1_result.intersection(&c2_result).cloned().collect()
            }

            Condition::Or(sub_conditions) => {
                let c1_result = self.filter_transactions(transactions, (*sub_conditions).0);
                let c2_result = self.filter_transactions(transactions, (*sub_conditions).1);
                c1_result.union(&c2_result).cloned().collect()
            }
        }
    }

    /// The new select implementation
    pub(crate) fn query(&mut self, from: Option<String>, condition: Option<Condition>) -> Vec<Transaction> {
        let mut trans :HashSet<u32> = match from {
            None => self.transactions.keys().cloned().collect::<HashSet<u32>>(),
            Some(account) => self.transactions.values().filter(|t| account == t.account).map(|t| t.id).collect()
        };

        if let Some(condition) = condition {
            trans = self.filter_transactions(&trans, condition);
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

    pub(crate) fn find_by_id(&self, id: u32) -> Transaction {
        let t = self.transactions.get(&id).unwrap();
        self.to_transaction(t)
    }

    pub(crate) fn search_by_id(&self, id: u32) -> Option<Transaction> {
        self.transactions.get(&id).map(|t| self.to_transaction(t))
    }

    pub(crate) fn delete(&mut self, ids: &[u32]) -> u32 {
        let mut trans_deleted: u32 = 0;
        for trans_id in ids {
            if self.delete_single(*trans_id) {
                trans_deleted += 1;
            }
        }
        self.save();
        trans_deleted
    }

    /// Delete a single transaction. Return true if transaction is found and deleted.
    /// This function DOES NOT save db. save() must be explicitly called to persist the delete.
    fn delete_single(&mut self, trans_id: u32) -> bool {
        if let Some(t) = self.transactions.remove(&trans_id) {
            // Remove transaction from date index
            self.date_index.entry(t.date.date()).and_modify(|bitmap| { bitmap.remove(trans_id); });

            // Remove transaction from label index
            for label_id in &*t.labels {
                self.label_id_to_transactions.entry(*label_id).and_modify(|bitmap| { bitmap.remove(trans_id); });
            }

            // Remove transaction from full text search index
            self.search_index.delete(trans_id, &t.description);

            true
        } else {
            false
        }
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
            labels: LabelIdVec::empty()
        };

        let s = serde_json::to_string::<TransactionRecord>(&t).unwrap();
        println!("{}", s);
    }
}
