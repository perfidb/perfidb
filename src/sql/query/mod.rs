use comfy_table::{Table, TableComponent};
use log::info;
use sqlparser::ast::{Query, SetExpr, TableFactor};
use crate::Database;

pub(crate) fn run_query(query: Box<Query>, db: &Database) {
    let Query { with, body, .. } = *query;
    if let SetExpr::Select(select) = body {
        info!("{:?}", select.projection);
        info!("{:?}", select.from);
        info!("{:?}", select.selection);

        let mut transactions = vec![];

        if let TableFactor::Table { name, .. } = &select.from[0].relation {
            let table_name = name.to_string();
            transactions = db.query(table_name.as_str(), select.selection.unwrap());
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