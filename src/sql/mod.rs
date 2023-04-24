mod select;
mod insert;
mod copy;
pub mod parser;



use crate::{Config, Database};

use crate::sql::parser::Statement::{Delete, Export, Import, Insert, Select, UpdateLabel};
use crate::tagger::Tagger;

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
                UpdateLabel(labels, condition) => {
                    if labels.to_ascii_lowercase() == "auto()" {
                        let auto_labeller = Tagger::new(&Config::load_from_file(auto_label_rules_file));
                        db.auto_label_new(&auto_labeller, condition);
                    } else {
                        let labels: Vec<&str> = labels.split(',').map(|t| t.trim()).collect();
                        // TODO: find a way to remove old labels
                        db.set_labels_for_multiple_transactions_new(&labels, condition);
                    }
                }
                Insert(account, records) => {
                    insert::execute_insert(db, account, records);
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
