pub(crate) mod select;

use std::collections::HashMap;
use chrono::NaiveDateTime;
use comfy_table::{Table, TableComponent, Cell, Color, CellAlignment};
use log::{warn};
use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, TableFactor, Value};
use crate::{Config, Database};
use crate::sql::parser::Projection;
use crate::tagger::Tagger;
use crate::transaction::Transaction;

pub(crate) fn run_query(query: Box<Query>, db: &mut Database, auto_label_config_file: &str) {
    let Query { with: _, body, .. } = *query;
    if let SetExpr::Select(select) = *body {

        if let TableFactor::Table { name, .. } = &select.from[0].relation {
            // assume it always has at least 1 identifier
            let table_name :&String = &name.0[0].value;

            let mut auto_labelling = false;
            if let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] {
                if func.name.0[0].value.to_ascii_lowercase() == "auto" {
                    auto_labelling = true;
                }
            }

            let mut transactions :Vec<Transaction> = vec![];
            let mut select_by_id = false;
            if select.projection.len() == 1 {
                // Handle  SELECT transaction_id FROM db
                if let SelectItem::UnnamedExpr(Expr::Value(Value::Number(string, _))) = &select.projection[0] {
                    select_by_id = true;
                    let trans_id = string.parse::<u32>().unwrap();
                    transactions = match db.search_by_id(trans_id) {
                        Some(t) => vec![t],
                        None => vec![]
                    };
                }
            }

            if !select_by_id {
                transactions = db.query(table_name.as_str(), select.selection);
            }

            if auto_labelling {
                let tagger = Tagger::new(&Config::load_from_file(auto_label_config_file));
                for t in transactions.iter_mut() {
                    let new_labels = tagger.label(t);
                    t.labels = new_labels;
                }
            }

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
        let projection :Projection = match &projection[0] {
            // SELECT * FROM ...
            SelectItem::Wildcard(_) => Projection::Star,
            // SELECT 123 FROM ...
            // Select by id has already been handled above
            SelectItem::UnnamedExpr(Expr::Value(Value::Number(num_string, _))) => Projection::Id(num_string.parse::<usize>().unwrap()),

            // SELECT SUM(*) FROM
            // SELECT COUNT(*) FROM
            SelectItem::UnnamedExpr(Expr::Function(func)) => {
                let func_name: String = func.name.0[0].value.to_ascii_uppercase();
                match func_name.as_str() {
                    "SUM" => Projection::Sum,
                    "COUNT" => Projection::Count,
                    "AUTO" => Projection::Auto,
                    _ => Projection::Star
                }
            },
            _ => Projection::Star
        };

        handle_normal_select(transactions, &mut table, projection);
    }
}

fn set_cell_style(t: &Transaction, cell: Cell, is_tagging: bool) -> Cell {
    if is_tagging && !t.labels.is_empty() {
        cell.fg(Color::Black).bg(Color::Green)
    } else {
        cell
    }
}

fn handle_normal_select(transactions: &[Transaction], table: &mut Table, projection: Projection) {
    let mut is_normal_select = false;
    let mut is_sum = false;
    let mut is_count = false;
    // Is auto labelling transactions
    let mut is_auto_labelling = false;

    match projection {
        // SELECT * FROM ...
        Projection::Star |
        // SELECT 123 FROM ...
        // Select by id has already been handled above
        Projection::Id(_) => is_normal_select = true,

        // SELECT SUM(*) FROM
        // SELECT COUNT(*) FROM
        Projection::Sum => is_sum = true,
        Projection::Count => is_count = true,
        Projection::Auto => {
            is_normal_select = true;
            is_auto_labelling = true;
        }
    }

    if is_normal_select {
        table.set_header(vec!["ID", "Account", "Date", "Description", "Amount", "Labels"]);

        for t in transactions {
            table.add_row(vec![
                set_cell_style(t, Cell::new(t.id.to_string().as_str()), is_auto_labelling).set_alignment(CellAlignment::Right),
                set_cell_style(t, Cell::new(t.account.as_str()), is_auto_labelling),
                set_cell_style(t, Cell::new(format_date(t.date).as_str()), is_auto_labelling),
                set_cell_style(t, Cell::new(t.description.as_str()), is_auto_labelling),
                set_cell_style(t, Cell::new(format_amount(t.amount).as_str()), is_auto_labelling).set_alignment(CellAlignment::Right),
                set_cell_style(t, Cell::new(t.tags_display().as_str()), is_auto_labelling)
            ]);
        }
    } else if is_sum {
        table.set_header(vec!["Subtotal"]);

        table.add_row(vec![Cell::new(format_amount(
            transactions.iter().map(|t| t.amount).fold(0.0, |total, amount| total + amount))
        ).set_alignment(CellAlignment::Right)]);
    } else if is_count {
        table.set_header(vec!["Count"]);
        table.add_row(vec![Cell::new(transactions.len()).set_alignment(CellAlignment::Right)]);
    }

    println!("{table}");
}

/// handles 'GROUP BY tags'
fn group_by_tags(transactions: &[Transaction], table: &mut Table) {
    table.set_header(vec!["Tag", "Amount"]);

    let mut group_by_map: HashMap<&str, f32> = HashMap::new();
    for t in transactions {
        for tag in &t.labels {
            let entry = group_by_map.entry(tag.as_str()).or_insert(0.0);
            *entry += t.amount;
        }
    }

    for (tag, amount) in group_by_map {
        table.add_row(vec![tag, amount.to_string().as_str()]);
    }

    println!("{table}");
}

/// Format $ amount
fn format_amount(amount: f32) -> String {
    format!("{amount:.2}")
}

fn format_date(date: NaiveDateTime) -> String {
    date.format("%Y-%m-%d").to_string()
}
