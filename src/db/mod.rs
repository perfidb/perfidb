mod filter;

use std::fs;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use chrono::{NaiveDate, NaiveDateTime};
use log::info;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr};
use sqlparser::ast::Expr::Identifier;

use crate::csv_reader::Record;
use crate::transaction::Transaction;


/// Internal representation of a transaction record in database
#[derive(Serialize, Deserialize, Debug)]
struct TransactionRecord {
    id: u32,
    account: String,
    date: NaiveDateTime,
    description: String,
    amount: f32,

    // List of tag ids
    tags: Vec<u32>,
}

impl TransactionRecord {
    pub(crate) fn has_tags(&self) -> bool {
        !self.tags.is_empty()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Database {
    transaction_id_seed: u32,
    transactions: HashMap<u32, TransactionRecord>,

    /// Key is transaction date, value is a list of transaction ids.
    date_index: BTreeMap<NaiveDate, Vec<u32>>,

    /// key is tag string, value is tag's index
    tag_name_to_id: HashMap<String, u32>,

    tag_id_to_name: HashMap<u32, String>,

    tag_id_seed: u32,

    /// tag id to a list of transactions with that tag
    tag_id_to_transactions: HashMap<u32, Vec<u32>>,

    /// Inverted index for full-text search on 'description'
    token_to_transactions: HashMap<String, HashSet<u32>>,

    #[serde(skip_serializing, skip_deserializing)]
    file_path: Option<String>,

    #[serde(skip_serializing, skip_deserializing)]
    pub(crate) last_query_results: Option<Vec<u32>>,
}

impl Database {
    pub(crate) fn new(file_path: Option<String>) -> Database {
        Database {
            transaction_id_seed: 1,
            transactions: HashMap::new(),
            date_index: BTreeMap::new(),
            tag_name_to_id: HashMap::new(),
            tag_id_to_name: HashMap::new(),
            tag_id_seed: 1,
            tag_id_to_transactions: HashMap::new(),
            token_to_transactions: HashMap::new(),
            file_path,
            last_query_results: None,
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


    pub(crate) fn upsert(&mut self, t: &Record) {
        let trans_id = self.transaction_id_seed;

        // increment seed
        self.transaction_id_seed += 1;

        let t = TransactionRecord {
            id: trans_id,
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            tags: vec![],
        };

        let date: NaiveDate = t.date.date();
        self.date_index.entry(date).or_insert(vec![]);

        // Add to date index
        self.date_index.get_mut(&date).unwrap().push(trans_id);

        self.index_description(trans_id, &t.description);

        // Add to transactions table
        self.transactions.insert(trans_id, t);
    }

    /// Tokenise description by whitespace and add trans_id into reverse index
    fn index_description(&mut self, trans_id: u32, description: &str) {
        for token in description.split_whitespace() {
            let token = token.to_lowercase();
            if !self.token_to_transactions.contains_key(token.as_str()) {
                self.token_to_transactions.insert(token.clone(), HashSet::new());
            }

            self.token_to_transactions.get_mut(&token).unwrap().insert(trans_id);
        }
    }

    pub(crate) fn update_tags(&mut self, trans_id: u32, tags: &String) {
        let tags: Vec<&str> = tags.split(',').map(|t| t.trim()).filter(|t| !t.is_empty()).collect();

        let mut existing_tags = HashSet::<String>::new();
        for tag_id in self.transactions.get(&trans_id).unwrap().tags.iter() {
            existing_tags.insert(self.tag_id_to_name.get(tag_id).unwrap().clone());
        }

        let mut tags_to_remove :Vec<&str> = vec![];
        let mut tags_to_add :Vec<&str> = vec![];
        for existing_tag in existing_tags.iter() {
            if !tags.contains(&existing_tag.as_str()) {
                tags_to_remove.push(existing_tag);
            }
        }
        for new_tag in tags {
            if !existing_tags.contains(new_tag) {
                tags_to_add.push(new_tag);
            }
        }

        if !tags_to_remove.is_empty() {
            self.remove_tags(trans_id, &tags_to_remove);
        }

        if !tags_to_add.is_empty() {
            self.add_tags(trans_id, &tags_to_add);
        }
    }

    pub(crate) fn add_tags(&mut self, trans_id: u32, tags: &[&str]) {
        info!("Adding tags {:?} for transaction {}", tags, trans_id);

        for tag in tags {
            if !self.tag_name_to_id.contains_key(*tag) {
                self.tag_name_to_id.insert(tag.to_string(), self.tag_id_seed);
                self.tag_id_to_name.insert(self.tag_id_seed, tag.to_string());
                self.tag_id_to_transactions.insert(self.tag_id_seed, vec![]);
                self.tag_id_seed += 1;

            }

            let tag_id = self.tag_name_to_id.get(*tag).unwrap();
            let transaction = self.transactions.get_mut(&trans_id).unwrap();
            if !transaction.tags.contains(tag_id) {
                transaction.tags.push(*tag_id);
                self.tag_id_to_transactions.get_mut(tag_id).unwrap().push(transaction.id);
            }
        }

        self.save();
    }

    pub(crate) fn update_tags_for_multiple_transactions(&mut self, where_clause: &Expr, tags: &[&str]) {
        let mut transactions = HashSet::<u32>::new();
        for trans_id in self.transactions.keys() {
            transactions.insert(*trans_id);
        }

        transactions = self.filter_transactions(&transactions, where_clause);

        for tag in tags {
            if !self.tag_name_to_id.contains_key(*tag) {
                self.tag_name_to_id.insert(tag.to_string(), self.tag_id_seed);
                self.tag_id_to_name.insert(self.tag_id_seed, tag.to_string());
                self.tag_id_to_transactions.insert(self.tag_id_seed, vec![]);
                self.tag_id_seed += 1;

            }

            let tag_id = self.tag_name_to_id.get(*tag).unwrap();
            for trans_id in &transactions {
                let transaction = self.transactions.get_mut(trans_id).unwrap();
                if !transaction.tags.contains(tag_id) {
                    transaction.tags.push(*tag_id);
                    self.tag_id_to_transactions.get_mut(tag_id).unwrap().push(transaction.id);
                }
            }
        }

        self.save();
    }

    pub(crate) fn remove_tags(&mut self, trans_id: u32, tags: &[&str]) {
        info!("Removing tags {:?} from transaction {}", tags, trans_id);
        let transaction = self.transactions.get_mut(&trans_id).unwrap();

        for tag in tags {
            // Only run if this tag id exists
            if let Some(tag_id_to_remove) = self.tag_name_to_id.get(*tag) {
                transaction.tags.retain(|tag_id| *tag_id != *tag_id_to_remove);
                // Remove transaction from dictionary
                self.tag_id_to_transactions.get_mut(tag_id_to_remove).unwrap().retain(|existing_trans_id| *existing_trans_id != trans_id);
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

                filter::handle_equals((*left).clone(), (*right).clone(), &self, &transactions)
            },

            Expr::BinaryOp{ left, op: BinaryOperator::NotEq, right } => {
                let left: &Expr = left;
                let right: &Expr = right;

                filter::handle_not_equal((*left).clone(), (*right).clone(), &self, &transactions)
            },

            // If it is 'LIKE' operator, we assume it's  description LIKE '...', so we don't check left
            Expr::BinaryOp{ left: _, op: BinaryOperator::Like, right} => {
                let right: &Expr = right;
                filter::handle_like((*right).clone(), &transactions, &self)
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
                let expr :&Expr = &(*expr);
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


    /// Current implementation is quite bad. Hope we can use a better way to do this in Rust
    pub(crate) fn query(&mut self, account: &str, where_clause: Option<Expr>) -> Vec<Transaction> {
        let mut transactions = if account == "db" {
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

    /// Save db content to disk
    pub(crate) fn save(&self) {
        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();
        fs::write((&self.file_path).as_ref().unwrap(), encoded).expect("Unable to write to database file");
    }

    fn to_transaction(&self, t: &TransactionRecord) -> Transaction {
        // TODO: use a function to format tags
        Transaction::new(t.id, t.account.clone(), t.date, t.description.as_str(), t.amount,
                         t.tags.iter().map(|tag_id| self.tag_id_to_name.get(tag_id).unwrap().clone()).collect::<Vec<String>>())
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
            tags: vec![]
        };

        let s = serde_json::to_string::<TransactionRecord>(&t).unwrap();
        println!("{}", s);
    }
}
