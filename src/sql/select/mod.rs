use std::collections::HashMap;
use chrono::NaiveDateTime;
use comfy_table::{Table, TableComponent, Cell, Color, CellAlignment};
use crate::transaction::Transaction;
use crate::config::Config;
use crate::db::Database;
use crate::sql::parser::{Condition, GroupBy, Projection};
use crate::tagger::Tagger;

/// Run an `SELECT` select
pub(crate) fn run_select(db: &mut Database, projection: Projection, from: Option<String>, condition: Option<Condition>, group_by: Option<GroupBy>, auto_label_rules_file: &str) {
    let mut transactions = db.query_new(from, condition);

    if let Projection::Auto = projection {
        let tagger = Tagger::new(&Config::load_from_file(auto_label_rules_file));
        for t in transactions.iter_mut() {
            let new_labels = tagger.label(t);
            t.labels = new_labels;
        }
    }

    process_projection(&projection, group_by, &transactions)
}

/// Print outputs based on select projection, e.g. SELECT *, SELECT SUM(*), etc
fn process_projection(projection: &Projection, group_by: Option<GroupBy>, transactions: &[Transaction]) {
    let mut table = Table::new();
    table.remove_style(TableComponent::HorizontalLines);
    table.remove_style(TableComponent::MiddleIntersections);
    table.remove_style(TableComponent::LeftBorderIntersections);
    table.remove_style(TableComponent::RightBorderIntersections);

    if group_by.is_some() {
        group_by_label(transactions, &mut table);
    } else {
        handle_normal_select(transactions, &mut table, projection);
    }
}

/// handles 'GROUP BY label'
fn group_by_label(transactions: &[Transaction], table: &mut Table) {
    table.set_header(vec!["Tag", "Amount"]);

    let mut group_by_map: HashMap<&str, f32> = HashMap::new();
    for t in transactions {
        for tag in &t.labels {
            let entry = group_by_map.entry(tag.as_str()).or_insert(0.0);
            *entry += t.amount;
        }
    }

    for (label, amount) in group_by_map {
        table.add_row(vec![
            Cell::new(label),
            Cell::new(format_amount(amount).as_str()).set_alignment(CellAlignment::Right)
        ]);
    }

    println!("{table}");
}

fn handle_normal_select(transactions: &[Transaction], table: &mut Table, projection: &Projection) {
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




fn set_cell_style(t: &Transaction, cell: Cell, is_tagging: bool) -> Cell {
    if is_tagging && !t.labels.is_empty() {
        cell.fg(Color::Black).bg(Color::Green)
    } else {
        cell
    }
}

/// Format $ amount
fn format_amount(amount: f32) -> String {
    format!("{amount:.2}")
}

fn format_date(date: NaiveDateTime) -> String {
    date.format("%Y-%m-%d").to_string()
}
