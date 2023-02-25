use std::collections::HashSet;
use sqlparser::ast::{Expr, Value};
use crate::db::Database;
use crate::db::search::SearchIndex;

/// Handles SQL LIKE where clause
pub(crate) fn handle_like(expr: Box<Expr>, transactions: &HashSet<u32>, database: &Database) -> HashSet<u32> {
    let keyword: String;

    // handle both with and without single quoted.
    if let Expr::Value(Value::SingleQuotedString(string)) = *expr {
        keyword = string;
    } else if let Expr::Identifier(ident) = *expr {
        keyword = ident.value;
    } else {
        return HashSet::new();
    }

    let mut search_index = SearchIndex::new();
    for (k, v) in database.transactions.iter() {
        search_index.index(v);
    }

    search_index.search(&keyword)


    // return match database.token_to_transactions.get(&keyword.to_lowercase()) {
    //     Some(trans_containing_token) => {
    //         transactions.intersection(trans_containing_token).cloned().collect()
    //     },
    //     None => HashSet::new()
    // };
}