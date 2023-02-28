use chrono::{NaiveDate};
use log::{info, warn};
use sqlparser::ast::{Expr, ObjectName, Query, SetExpr};
use crate::csv_reader::Record;
use crate::Database;
use crate::sql::util::{expr_to_float, expr_to_s};

/// Execute an INSERT statement
pub(crate) fn execute_insert(db : &mut Database, table: ObjectName, source: Box<Query>) {
    let account_name = table.to_string();

    match *(*source).body {
        SetExpr::Values(values) => {
            // SQL supports inserting multiple rows in one INSERT statement, i.e.
            // INSERT INTO account1 VALUES ('2022-01-02', 'food', -30.4), ('2022-01-02', 'petrol', -20)
            // Each row below is one transaction
            let mut total_inserted = 0;
            for row in values.rows {
                if row.len() != 3 {
                    warn!("Skipping invalid row: {}", format_row(&row));
                }

                let date = NaiveDate::parse_from_str(&expr_to_s(&row[0]).unwrap(), "%Y-%m-%d");
                if let Err(error) = date {
                    warn!("Unable to parse date, error: {}", error);
                    continue;
                }

                let new_record = Record {
                    id: None,
                    account: account_name.clone(),
                    date: date.unwrap().and_hms(0, 0, 0),
                    description: expr_to_s(&row[1]).unwrap(),
                    amount: expr_to_float(&row[2]).unwrap(),
                    labels: None,
                };

                db.upsert(&new_record);
                total_inserted += 1;
            }

            info!("Inserted {} transactions", total_inserted);
        },
        _ => {
            warn!("INSERT statement's body must be:  VALUES(date, description, amount, labels)  but your INSERT statement's body is: {}", source.body);
        }
    }
}

fn format_row(row: &[Expr]) -> String {
    format!("({})", row.iter().map(|expr| expr.to_string()).collect::<Vec<String>>().join(", "))
}