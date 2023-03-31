use std::collections::HashSet;
use sqlparser::ast::{Expr, Value};
use crate::db::Database;

/// Handles SQL LIKE where clause
pub(crate) fn handle_like(expr: Expr, database: &Database) -> HashSet<u32> {
    let keyword: String;

    // handle both with and without single quoted.
    if let Expr::Value(Value::SingleQuotedString(string)) = expr {
        keyword = string;
    } else if let Expr::Identifier(ident) = expr {
        keyword = ident.value;
    } else {
        return HashSet::new();
    }

    database.search_index.search(&keyword)
}
