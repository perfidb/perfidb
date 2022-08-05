use std::collections::HashSet;
use chrono::NaiveDateTime;

#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    pub(crate) kind: String,
}
