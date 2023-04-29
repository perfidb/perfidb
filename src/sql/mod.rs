mod select;
mod insert;
mod copy;
pub mod parser;

use log::info;
use crate::{Database};

use crate::sql::parser::Statement::{Delete, Export, Import, Insert, Select, Label};

pub(crate) fn parse_and_run_sql(db: &mut Database, sql: String, auto_label_rules_file: &str) -> Result<(), String> {
    // First use our own parser to parse
    let result = parser::parse(&sql);

    match result {
        Ok((_input, statement)) => {
            match statement {
                Export(file_path) => {
                    copy::execute_export_db(db, &file_path);
                }
                Import(account, file_path, inverse_amount, dryrun) => {
                    copy::execute_import(db, &account, &file_path, inverse_amount, dryrun);
                }
                Select(projection, from, condition, group_by) => {
                    select::run_select(db, projection, from, condition, group_by, auto_label_rules_file);
                }
                Label(trans_ids, label_cmd) => {
                    for trans_id in trans_ids {
                        // TODO: avoid copying vec multiple times
                        db.apply_label_ops(trans_id, label_cmd.clone(), auto_label_rules_file)
                    }
                    info!("\nLabel operations completed.")
                }
                Insert(account, records) => {
                    let records_count = insert::execute_insert(db, account, records);
                    info!("\n{records_count} transactions inserted.");
                }
                Delete(trans_ids) => {
                    db.delete(&trans_ids);
                }
            }
        },
        Err(e) => {
            return Err(e.to_string());
        }
    }

    Ok(())
}
