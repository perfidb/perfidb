use crate::db::Database;

pub(crate) fn run_select(db: &mut Database) {
    db.query("db", None);
}