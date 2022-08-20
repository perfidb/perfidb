use std::collections::HashMap;
use comfy_table::{Table, TableComponent};
use log::{info, warn};
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, TableFactor};
use crate::Database;
use crate::transaction::Transaction;

pub(crate) fn run_query(query: Box<Query>, db: &Database) {
    let Query { with: _, body, .. } = *query;
    if let SetExpr::Select(select) = body {
        info!("{:?}", select.group_by);

        if let TableFactor::Table { name, .. } = &select.from[0].relation {
            // assume it always has at least 1 identifier
            let table_name :&String = &name.0[0].value;

            let transactions :Vec<Transaction> = db.query(table_name.as_str(), select.selection);
            process_projection(&select.projection, &select.group_by, &transactions);
        }
    }
}

/// Print outputs based on query projection, e.g. SELECT *, SELECT SUM(*), etc
fn process_projection(projection: &[SelectItem], group_by: &[Expr], transactions: &[Transaction]) {
    if group_by.len() > 1 {
        warn!("GROUP BY more than one column is currently not supported");
        return;
    }

    let mut table = Table::new();
    table.remove_style(TableComponent::HorizontalLines);
    table.remove_style(TableComponent::MiddleIntersections);
    table.remove_style(TableComponent::LeftBorderIntersections);
    table.remove_style(TableComponent::RightBorderIntersections);


    // if 'GROUP BY tags'
    if group_by.len() == 1 {
        if let Expr::Identifier(ident) = &group_by[0] {
            if ident.value.to_ascii_lowercase() == "tags" {
                group_by_tags(transactions, &mut table);
            }
        }
    } else {
        handle_normal_select(transactions, &mut table, projection);
    }
}

fn handle_normal_select(transactions: &[Transaction], table: &mut Table, projection: &[SelectItem]) {
    table.set_header(vec!["ID", "Account", "Date", "Description", "Amount", "Tags"]);
    for t in transactions {
        table.add_row(vec![t.id.to_string().as_str(), t.account.as_str(), t.date.to_string().as_str(), t.description.as_str(), t.amount.to_string().as_str(), t.tags_display().as_str()]);
    }

    match &projection[0] {
        // SELECT * FROM ...
        SelectItem::Wildcard => {
            println!("{table}");
        },

        // SELECT SUM(*) FROM
        SelectItem::UnnamedExpr(Expr::Function(func)) => {
            if func.name.0[0].value.to_ascii_uppercase() == "SUM" {
                table.add_row(vec!["", "", "", "", "", ""]);
                table.add_row(vec!["", "", "", "Subtotal", transactions.iter().map(|t| t.amount).fold(0.0, |total, amount| total + amount).to_string().as_str(), ""]);
                println!("{table}");
            }
        },
        _ => {}
    }
}

/// handles 'GROUP BY tags'
fn group_by_tags(transactions: &[Transaction], table: &mut Table) {
    table.set_header(vec!["Tag", "Amount"]);

    let mut group_by_map: HashMap<&str, f32> = HashMap::new();
    for t in transactions {
        for tag in &t.tags {
            let entry = group_by_map.entry(tag.as_str()).or_insert(0.0);
            *entry += t.amount;
        }
    }

    for (tag, amount) in group_by_map {
        table.add_row(vec![tag, amount.to_string().as_str()]);
    }

    println!("{table}");
}