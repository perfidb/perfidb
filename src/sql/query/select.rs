use comfy_table::{Cell, CellAlignment, Table, TableComponent};
use crate::config::Config;
use crate::db::Database;
use crate::sql::parser::{Condition, Projection};
use crate::sql::query::{format_amount, format_date, set_cell_style};
use crate::tagger::Tagger;
use crate::transaction::Transaction;

pub(crate) fn run_select(db: &mut Database, projection: Projection, from: Option<String>, condition: Option<Condition>, auto_label_rules_file: &str) {
    let mut transactions = db.query_new(from, condition);

    if let Projection::Auto = projection {
        let tagger = Tagger::new(&Config::load_from_file(auto_label_rules_file));
        for t in transactions.iter_mut() {
            let new_labels = tagger.label(t);
            t.labels = new_labels;
        }
    }

    process_projection(&projection, &transactions)
}

/// Print outputs based on query projection, e.g. SELECT *, SELECT SUM(*), etc
fn process_projection(projection: &Projection, transactions: &[Transaction]) {
// TODO: projection
//     if group_by.len() == 1 {
//         if let Expr::Identifier(ident) = &group_by[0] {
//             if ident.value.to_ascii_lowercase() == "tags" {
//                 group_by_tags(transactions, &mut table);
//             }
//         }
//     }


    let mut table = Table::new();
    table.remove_style(TableComponent::HorizontalLines);
    table.remove_style(TableComponent::MiddleIntersections);
    table.remove_style(TableComponent::LeftBorderIntersections);
    table.remove_style(TableComponent::RightBorderIntersections);

    handle_normal_select(transactions, &mut table, projection);
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