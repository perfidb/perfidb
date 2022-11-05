mod unequal;

use std::collections::HashSet;
use chrono::{Datelike, NaiveDate, Utc};
use sqlparser::ast::{Expr, Value};
use sqlparser::ast::Expr::Identifier;
use crate::Database;

pub(crate) use unequal::handle_unequal;

pub(crate) fn handle_equals(left: Expr, right: Expr, database: &Database, transactions: &HashSet<u32>) -> HashSet<u32> {
    match left {
        Identifier(ident) => {
            match ident.value.to_lowercase().as_str() {
                // WHERE label = '...'
                "label" => {
                    if let Expr::Value(Value::SingleQuotedString(tag)) = right {
                        return match database.tag_name_to_id.get(&tag) {
                            Some(tag_id) => {
                                transactions.iter().filter(|id| database.transactions.get(id).unwrap().tags.contains(tag_id)).cloned().collect::<HashSet<u32>>()
                            },
                            None => HashSet::new()
                        };
                    }
                },

                "date" => {
                    if let Expr::Value(Value::Number(num_str, _)) = right {
                        let date = num_str.parse::<u32>().unwrap();
                        // if month
                        if date >= 1 && date <= 12 {
                            let month = date;
                            let today = Utc::now().naive_utc().date();
                            let mut year = today.year();
                            if month >= today.month() {
                                year -= 1;
                            }

                            let first_day = NaiveDate::from_ymd(year, month, 1);
                            let next_month = if month == 12 { 1 } else { month + 1 };
                            let next_month_year = if month == 12 { year + 1 } else { year };
                            let first_day_next_month = NaiveDate::from_ymd(next_month_year, next_month, 1);

                            let mut trans_in_date_range = HashSet::<u32>::new();
                            for (_, trans_ids) in database.date_index.range(first_day..first_day_next_month) {
                                for id in trans_ids {
                                    trans_in_date_range.insert(*id);
                                }
                            }

                            return transactions.intersection(&trans_in_date_range).cloned().collect();
                        }
                    }
                },

                &_ => {}
            }
        }
        _ => {}
    }

    HashSet::new()
}
