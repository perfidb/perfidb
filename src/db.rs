use std::{fmt, fs};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Utc};
use log::info;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{BinaryOperator, Expr, FunctionArg, FunctionArgExpr, Value};
use sqlparser::ast::Expr::Identifier;
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

    // List of tag ids
    tags: Vec<u32>,
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

    #[serde(skip_serializing, skip_deserializing)]
    file_path: Option<String>,
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
        if !self.date_index.contains_key(&date) {
            self.date_index.insert(date, vec![]);
        };
        // Add to date index
        self.date_index.get_mut(&date).unwrap().push(trans_id);
        // Add to transactions table
        self.transactions.insert(trans_id, t);
    }

    pub(crate) fn update_tags(&mut self, trans_id: u32, tags: &Vec<&str>) {
        info!("Updating tags {:?} for transaction {}", tags, trans_id);

        for tag in tags {
            if !self.tag_name_to_id.contains_key(*tag) {
                self.tag_name_to_id.insert(tag.to_string(), self.tag_id_seed);
                self.tag_id_to_name.insert(self.tag_id_seed, tag.to_string());
                self.tag_id_to_transactions.insert(self.tag_id_seed, vec![]);
                self.tag_id_seed += 1;

            }

            let tag_id = self.tag_name_to_id.get(*tag).unwrap();
            let mut transaction = self.transactions.get_mut(&trans_id).unwrap();
            if !transaction.tags.contains(tag_id) {
                transaction.tags.push(*tag_id);
                self.tag_id_to_transactions.get_mut(tag_id).unwrap().push(transaction.id);
            }
        }
    }

    pub(crate) fn remove_tags(&mut self, trans_id: u32, tags: &Vec<&str>) {
        info!("Removing tags {:?} from transaction {}", tags, trans_id);
        let mut transaction = self.transactions.get_mut(&trans_id).unwrap();

        for tag in tags {
            // Only run if this tag id exists
            if let Some(tag_id_to_remove) = self.tag_name_to_id.get(*tag) {
                transaction.tags.retain(|tag_id| *tag_id != *tag_id_to_remove);
                // Remove transaction from dictionary
                self.tag_id_to_transactions.get_mut(tag_id_to_remove).unwrap().retain(|existing_trans_id| *existing_trans_id != trans_id);
            }
        }
    }

    fn filter_transactions(&self, transactions: &HashSet<u32>, where_clause: &Expr) -> HashSet<u32> {
        match where_clause {
            // Expr::BinaryOp{ left: Expr::Identifier(ident) , op: BinaryOperator::Eq, right: Expr::Value(Value::SingleQuotedString(tag))} => {
            Expr::BinaryOp{ left, op: BinaryOperator::Eq, right} => {
                let left: &Expr = left;
                let right: &Expr = right;

                if let Identifier(ident) = left {
                    if ident.value == "tags" {
                        if let Expr::Value(Value::SingleQuotedString(tag)) = right {
                            return match self.tag_name_to_id.get(tag) {
                                Some(tag_id) => {
                                    let mut results = HashSet::<u32>::new();
                                    for trans_id in self.tag_id_to_transactions.get(tag_id).unwrap() {
                                        results.insert(*trans_id);
                                    }
                                    results
                                },
                                None => HashSet::new()
                            };
                        }
                    }
                }

                HashSet::new()
            },
            Expr::BinaryOp{ left, op, right} => {
                match op {
                    BinaryOperator::And => {
                        let left_result = self.filter_transactions(transactions, left);
                        let right_result = self.filter_transactions(transactions, right);
                        left_result.intersection(&right_result).cloned().collect()
                    },
                    BinaryOperator::Or => {
                        let left_result = self.filter_transactions(transactions, left);
                        let right_result = self.filter_transactions(transactions, right);
                        left_result.union(&right_result).cloned().collect()
                    },
                    BinaryOperator::Gt => {
                        let s: String = right.to_string();
                        let amount_limit = s.parse::<f32>().unwrap();

                        transactions.iter().filter(|id| self.transactions.get(id).unwrap().amount.abs() > amount_limit).cloned().collect::<HashSet<u32>>()
                    },
                    _ => HashSet::new()
                }
            },

            Expr::Function(f) => {
                let func_name: &String = &f.name.0[0].value;
                if func_name.eq("month") {
                    if f.args.len() == 1 {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(value))) = &f.args[0] {
                            match value {
                                Value::Number(number, _) => {
                                    let month = number.parse::<u32>().unwrap();
                                    let today = Utc::now().naive_utc().date();
                                    let mut year = today.year();
                                    if month >= today.month() {
                                        year -= 1;
                                    }

                                    let first_day = NaiveDate::from_ymd(year, month, 1);
                                    let next_month = if month == 12 { 1 } else { month + 1 };
                                    let next_month_year = if month == 12 { year + 1 } else { year };
                                    let first_day_next_month = NaiveDate::from_ymd(next_month_year, next_month, 1);

                                    let mut transactions = HashSet::<u32>::new();
                                    for (_, trans_ids) in self.date_index.range(first_day..first_day_next_month) {
                                        for id in trans_ids {
                                            transactions.insert(*id);
                                        }
                                    }

                                    info!("{} {}", first_day, first_day_next_month);

                                    return transactions;
                                },
                                _ => {
                                    return HashSet::new();
                                }
                            }
                        }
                    }
                }
                HashSet::new()
            },
            _ => HashSet::new()
        }
    }


    /// Current implementation is quite bad. Hope we can use a better way to do this in Rust
    pub(crate) fn query(&self, account: &str, where_clause: Option<Expr>) -> Vec<Transaction> {
        let mut transactions = self.transactions.values().filter(|t| {
            account == "all" || account == t.account
        }).map(|t| t.id).collect::<HashSet<u32>>();

        // TODO: half implemented 'amount > ...'
        if let Some(where_clause) = where_clause {
            transactions = self.filter_transactions(&transactions, &where_clause);
        }

        let mut transactions = transactions.iter().map(|id| self.transactions.get(id).unwrap()).collect::<Vec<&TransactionRecord>>();

        transactions.sort_by(|a, b| a.date.partial_cmp(&b.date).unwrap());

        transactions.iter().map(|t| Transaction {
            id: t.id,
            account: t.account.clone(),
            date: t.date,
            description: t.description.clone(),
            amount: t.amount,
            tags: t.tags.iter().map(|tag_id| self.tag_id_to_name.get(tag_id).unwrap().as_str()).collect::<Vec<&str>>().join(", "),
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
