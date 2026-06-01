use comfy_table::{Table, TableComponent};
use log::info;

use crate::db::Database;
use crate::tokeniser;

/// Re-process (re-index) the given transactions using the current tokeniser.
///
/// In dry-run mode it prints the tokens each transaction would produce without modifying the
/// search index. Otherwise it re-indexes the transactions and persists the database.
pub(crate) fn run(db: &mut Database, ids: &[u32], dry_run: bool) {
    if dry_run {
        let mut table = Table::new();
        table.set_header(vec!["Id", "Description", "Tokens"]);
        table.remove_style(TableComponent::HorizontalLines);
        table.remove_style(TableComponent::MiddleIntersections);
        table.remove_style(TableComponent::LeftBorderIntersections);
        table.remove_style(TableComponent::RightBorderIntersections);
        for id in ids {
            if let Some(t) = db.search_by_id(*id) {
                let tokens = tokeniser::tokenise(&t.description);
                table.add_row(vec![t.id.to_string(), t.description, tokens.join(", ")]);
            }
        }
        println!("{table}");
        info!("This is a dry-run. The search index is not modified.");
    } else {
        let n = db.reprocess(ids);
        info!("Re-indexed {n} transactions.");
    }
}
