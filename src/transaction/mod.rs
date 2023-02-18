use chrono::NaiveDateTime;
use serde::Serializer;

/// Hold transaction info returned from database query
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct Transaction {
    pub(crate) id: u32,
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    #[serde(serialize_with = "serialise_tags")]
    pub(crate) tags: Vec<String>,
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
            tags,
        }
    }

    pub(crate) fn tags_display(&self) -> String {
        self.tags.join(", ")
    }
}

/// Join all tags by a bar |
fn serialise_tags<S>(tags: &[String], serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
    serializer.collect_str(tags.join("|").as_str())
}
