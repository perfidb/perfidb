use std::collections::HashSet;
use std::ops::{Add, Range};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use log::{warn};
use regex::Regex;
use sqlparser::ast::{Expr, Value};
use sqlparser::ast::Expr::Identifier;
use sqlparser::ast::Value::Number;
use crate::db::Database;

pub(crate) fn handle_equals(left: Expr, right: Expr, database: &Database, transactions: &HashSet<u32>) -> HashSet<u32> {
    if let Identifier(ident) = left {
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
                match parse_date_range(&right) {
                    Ok(date_range) => {
                        let mut trans_in_date_range = HashSet::<u32>::new();
                        for (_, trans_ids) in database.date_index.range(date_range) {
                            for id in trans_ids {
                                trans_in_date_range.insert(*id);
                            }
                        }
                        return transactions.intersection(&trans_in_date_range).cloned().collect();
                    },
                    Err(()) => {
                        warn!("Unable to parse date {}", right.to_string());
                    }
                }
            },

            // WHERE id = 123
            "id" => {
                if let Expr::Value(Number(string, _)) = right {
                    let trans_id = string.parse::<u32>().unwrap();
                    return vec![trans_id].into_iter().collect();
                }
            }

            &_ => {}
        }
    }

    HashSet::new()
}

fn parse_date_range(date_expr: &Expr) -> Result<Range<NaiveDate>, ()> {
    if let Expr::Value(Value::Number(num_str, _)) = date_expr {
        let date = num_str.parse::<u32>().unwrap();
        // if month
        if (1..=12).contains(&date) {
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

            return Ok(first_day..first_day_next_month);
        }
    } else if let Expr::Value(Value::SingleQuotedString(string)) = date_expr {
        // Handle date format '2022-09-03'
        let yyyy_mm_dd = Regex::new(r"^\d{4}-\d{2}-\d{2}$").unwrap();
        if yyyy_mm_dd.is_match(string) {
            let date = NaiveDate::parse_from_str(string.as_str(), "%Y-%m-%d").unwrap();
            return Ok(date..date.add(Duration::days(1)));
        }

        let yyyy_mm = Regex::new(r"^\d{4}-\d{2}$").unwrap();
        if yyyy_mm.is_match(string) {
            let splitted: Vec<&str> = string.split('-').collect();
            let year = splitted[0].to_string().parse::<i32>().unwrap();
            let month = splitted[1].to_string().parse::<u32>().unwrap();

            let first_day = NaiveDate::from_ymd(year, month, 1);
            let next_month = if month == 12 { 1 } else { month + 1 };
            let next_month_year = if month == 12 { year + 1 } else { year };
            let first_day_next_month = NaiveDate::from_ymd(next_month_year, next_month, 1);

            return Ok(first_day..first_day_next_month);
        }
    }

    Err(())
}
