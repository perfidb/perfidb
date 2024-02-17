use std::path::PathBuf;
use log::info;
use crate::db::Database;
use crate::import;
use crate::parser;
use crate::parser::Statement::{Delete, Export, Import, Insert, Label, Select};

mod export;
mod select;
mod insert;

pub(crate) fn parse_and_run_command(db: &mut Database, import_root_dir: &PathBuf, sql: String, auto_label_rules_file: &str) -> Result<(), String> {
    // First use our own parser to parse
    let result = parser::parse(&sql);

    match result {
        Ok((_input, statement)) => {
            match statement {
                Export(file_path) => {
                    export::execute_export_db(db, &file_path);
                }
                Import(inverse_amount, dryrun) => {
                    import::execute_import(db, import_root_dir, inverse_amount, dryrun);
                }
                Select(projection, from, condition, order_by, limit, group_by) => {
                    select::run_select(db, projection, from, condition, order_by, limit, group_by, auto_label_rules_file);
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
                    match trans_ids {
                        Some(trans_ids) => {
                            let trans_deleted = db.delete(&trans_ids);
                            info!("{trans_deleted} transactions deleted.");
                        },
                        None => info!("Unable to parse transaction IDs to delete, ignore operation.")
                    }
                }
            }
        },
        Err(e) => {
            return Err(e.to_string());
        }
    }

    info!("\n");

    Ok(())
}