use std::fs;
use std::path::Path;
use serde::Deserialize;
use toml::value::Table;

#[derive(Deserialize, Debug)]
pub(crate) struct Config {
    pub(crate) labels: Table
}

impl Config {
    pub(crate) fn empty() -> Config {
        Config { labels: Table::new() }
    }

    pub(crate) fn load_from_file(file_path: &str) -> Config {
        let path = Path::new(file_path);
        if path.exists() && path.is_file() {
            let config :Config = toml::from_slice::<Config>(&fs::read(path).unwrap()).unwrap();
            config
        } else {
            Config::empty()
        }
    }
}

