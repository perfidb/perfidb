use std::collections::HashSet;
use sqlparser::ast::{Expr, Value};
use sqlparser::ast::Expr::Identifier;
use crate::Database;

pub(crate) fn handle_not_equal(left: Expr, right: Expr, database: &Database, transactions: &HashSet<u32>) -> HashSet<u32> {
    if let Identifier(ident) = left {
        match ident.value.to_lowercase().as_str() {
            // WHERE label != '...'
            "label" => {
                if let Expr::Value(Value::SingleQuotedString(tag)) = right {
                    return match database.label_minhash.lookup_by_string(tag) {
                        Some(tag_id) => {
                            transactions.iter().filter(|id| !database.transactions.get(id).unwrap().labels.contains(&tag_id)).cloned().collect::<HashSet<u32>>()
                        },
                        None => HashSet::new()
                    };
                }
            },

            &_ => {}
        }
    }

    HashSet::new()
}
