use std::collections::HashSet;
use sqlparser::ast::{Expr, Value};
use sqlparser::ast::Expr::Identifier;
use crate::Database;

pub(crate) fn handle_unequal(left: Expr, right: Expr, database: &Database, transactions: &HashSet<u32>) -> HashSet<u32> {
    match left {
        Identifier(ident) => {
            match ident.value.to_lowercase().as_str() {
                // WHERE label != '...'
                "label" => {
                    if let Expr::Value(Value::SingleQuotedString(tag)) = right {
                        return match database.tag_name_to_id.get(&tag) {
                            Some(tag_id) => {
                                transactions.iter().filter(|id| !database.transactions.get(id).unwrap().tags.contains(tag_id)).cloned().collect::<HashSet<u32>>()
                            },
                            None => HashSet::new()
                        };
                    }
                },

                &_ => {}
            }
        }
        _ => {}
    }

    HashSet::new()
}
