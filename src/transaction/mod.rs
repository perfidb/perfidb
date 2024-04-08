use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;
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

/// A hash function based on a transaction's content.
/// We use amount's absolute value because sometimes we need to deal with inverted amount,
/// e.g. in the statement we have $96 but the same transaction already imported had -$96,
/// if both transactions have the same date and description we want the hash to be the same.
pub(crate) fn transaction_hash(datetime: NaiveDateTime, description: &str, amount: f32) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write_i64(datetime.and_utc().timestamp());
    hasher.write(description.as_bytes());
    hasher.write(&amount.abs().to_le_bytes());

    hasher.finish()
}

/// Join all tags by a bar |
fn serialise_labels<S>(tags: &[String], serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
    serializer.collect_str(tags.join("|").as_str())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use chrono::NaiveDateTime;
    use crate::transaction::transaction_hash;

    #[test]
    fn test() {
        let datetime1 = NaiveDateTime::from_str("2023-10-11T11:15:34").unwrap();
        let datetime2 = NaiveDateTime::from_str("2023-10-11T11:15:35").unwrap();
        assert_eq!(
            transaction_hash(datetime1, "Buy milk", 32.0),
            transaction_hash(datetime1, "Buy milk", 32.0)
        );

        // Verify inverted amount results same hash
        assert_eq!(
            transaction_hash(datetime1, "Buy milk", 32.56),
            transaction_hash(datetime1, "Buy milk", -32.56)
        );

        assert_ne!(
            transaction_hash(datetime1, "Buy milk", 32.0),
            transaction_hash(datetime2, "Buy milk", 32.0)
        );
    }
}