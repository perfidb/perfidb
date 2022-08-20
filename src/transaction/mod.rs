use chrono::NaiveDateTime;

#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) id: u32,
    pub(crate) account: String,
    pub(crate) date: NaiveDateTime,
    pub(crate) description: String,
    pub(crate) amount: f32,
    pub(crate) tags: Vec<String>,
}

impl Transaction {
    pub(crate) fn tags_display(&self) -> String {
        self.tags.join(", ")
    }
}
