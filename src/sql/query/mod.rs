pub(crate) mod select;

use std::collections::HashMap;
use chrono::NaiveDateTime;
use comfy_table::{Table, TableComponent, Cell, Color, CellAlignment};
use log::{warn};
use crate::sql::parser::Projection;
use crate::transaction::Transaction;

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
        Projection::Sum(_) => is_sum = true,
        Projection::Count(_) => is_count = true,
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
