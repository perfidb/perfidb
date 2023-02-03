use chrono::NaiveDateTime;
use log::{info, warn};
use sqlparser::ast::{Expr, ObjectName, Query, SetExpr};
use crate::csv_reader::Record;
use crate::Database;

/// Execute an INSERT statement
pub(crate) fn execute_insert(db : &mut Database, table: ObjectName, source: Box<Query>) {
    let account_name = table.to_string();

    match source.body {
        SetExpr::Values(values) => {
            // SQL supports inserting multiple rows in one INSERT statement, i.e.
            // INSERT INTO account1 VALUES ('2022-01-02', 'food', -30.4), ('2022-01-02', 'petrol', -20)
            // Each row below is one transaction
            let mut total_inserted = 0;
            for row in values.0 {
                if row.len() != 3 {
                    warn!("Skipping invalid row: {}", format_row(&row));
                }

                println!("{}", row[0].to_string());

                let new_record = Record {
                    account: account_name.clone(),
                    date: NaiveDateTime::parse_from_str(&row[0].to_string(), "%Y-%m-%d").unwrap(),
                    description: row[1].to_string(),
                    amount: row[2].to_string().parse::<f32>().unwrap()
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

fn format_row(row: &Vec<Expr>) -> String {
    format!("({})", row.iter().map(|expr| expr.to_string()).collect::<Vec<String>>().join(", "))
}