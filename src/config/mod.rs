use serde::Deserialize;
use toml::value::Table;

#[derive(Deserialize, Debug)]
pub(crate) struct Config {
    pub(crate) tags: Table
}

impl Config {
    pub(crate) fn empty() -> Config {
        Config { tags: Table::new() }
    }
}

