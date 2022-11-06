use std::collections::HashSet;
use sqlparser::ast::{BinaryOperator, Expr, Value};
use crate::db::Database;

/// Handles SQL LIKE where clause
pub(crate) fn handle_like(right: Expr, transactions: &HashSet<u32>, database: &Database) -> HashSet<u32> {
    let keyword: String;

    // handle both with and without single quoted.
    if let Expr::Value(Value::SingleQuotedString(string)) = right {
        keyword = string.clone();
    } else if let Expr::Identifier(ident) = right {
        keyword = ident.value.clone();
    } else {
        return HashSet::new();
    }

    return match database.token_to_transactions.get(&keyword.to_lowercase()) {
        Some(trans_containing_token) => {
            transactions.intersection(trans_containing_token).cloned().collect()
        },
        None => HashSet::new()
    };
}