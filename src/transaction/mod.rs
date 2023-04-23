use chrono::NaiveDateTime;
use serde::Serializer;

/// Hold transaction info returned from database select
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Transaction {
    #[serde(alias = "_perfidb_transaction_id", rename(serialize = "_perfidb_transaction_id"))]
    pub(crate) id: u32,
    #[serde(alias = "_perfidb_account", rename(serialize = "_perfidb_account"))]
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    #[serde(serialize_with = "serialise_labels", rename(serialize = "_perfidb_label"))]
    pub(crate) labels: Vec<String>,
}

impl Transaction {
    pub(crate) fn new(id: u32, account: String, date: NaiveDateTime, description: &str, amount: f32, tags: Vec<String>) -> Transaction {
        let description = description.replace('\n', " ");
        Transaction {
            id,
            account,
            date,
            description,
            amount,
            labels: tags,
        }
    }

    pub(crate) fn tags_display(&self) -> String {
        self.labels.join(", ")
    }
}

/// Join all tags by a bar |
fn serialise_labels<S>(tags: &[String], serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
    serializer.collect_str(tags.join("|").as_str())
}
