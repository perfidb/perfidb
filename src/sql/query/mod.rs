use comfy_table::{Table, TableComponent};
use log::info;
use sqlparser::ast::{Query, SetExpr, TableFactor};
use crate::Database;
use crate::transaction::Transaction;

pub(crate) fn run_query(query: Box<Query>, db: &Database) {
    let Query { with: _, body, .. } = *query;
    if let SetExpr::Select(select) = body {
        info!("{:?}", select.projection);
        info!("{:?}", select.from);
        info!("{:?}", select.selection);

        let mut transactions :Vec<Transaction> = vec![];

        if let TableFactor::Table { name, .. } = &select.from[0].relation {
            // assume it always has at least 1 identifier
            let table_name :&String = &name.0[0].value;

            // // parse 'WHERE' clause if there is one
            // if let Some(selection) = select.selection {
            //     // TODO: Handle AND, OR, predicate
            // }

            transactions = db.query(table_name.as_str(), select.selection);
        }

        let mut table = Table::new();
        table.remove_style(TableComponent::HorizontalLines);
        table.remove_style(TableComponent::MiddleIntersections);
        table.remove_style(TableComponent::LeftBorderIntersections);
        table.remove_style(TableComponent::RightBorderIntersections);
        table.set_header(vec!["Account", "Date", "Description", "Amount", "Tags"]);
        for t in transactions {
            // TODO handle tags
            table.add_row(vec![t.account.as_str(), t.date.to_string().as_str(), t.description.as_str(), t.amount.to_string().as_str(), ""]);
        }

        println!("{table}");
    }
}