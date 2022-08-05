use clap::lazy_static::lazy_static;
use regex::Regex;
use crate::db::TransactionKind;
use crate::transaction::Transaction;

const TRANSFER_PATTERNS: [&str; 3] = [
    "transfer$",
    "DIRECT DEBIT RECEIVED",
    "ONLINE PAYMENT RECEIVED",
];

const SALARY_PATTERNS: [&str; 4] = [
    "jobseeker", "seek", "telstra", "readify"
];

lazy_static! {
    static ref TRANSFER_REGEX: Regex = Regex::new(&("(?i)".to_string() + &TRANSFER_PATTERNS.join("|"))).unwrap();
    static ref SALARY_REGEX: Regex = Regex::new(&("(?i)".to_string() + &SALARY_PATTERNS.join("|"))).unwrap();
}

pub(crate) fn enrich(t: &Transaction) -> TransactionKind {
    if TRANSFER_REGEX.is_match(&t.description) {
        return TransactionKind::Transfer;
    } else if t.amount > 300.0 && SALARY_REGEX.is_match(&t.description) {
        return TransactionKind::Income;
    } else if t.amount < -8000.0 {
        return TransactionKind::LargeExpense
    }

    TransactionKind::Expense
}