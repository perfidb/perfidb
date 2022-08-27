use std::collections::HashMap;
use regex::Regex;
use toml::Value;
use crate::Config;
use crate::transaction::Transaction;

pub(crate) struct Tagger {
    tag_regex_map: HashMap<String, Vec<Regex>>
}

impl Tagger {
    pub(crate) fn new(config: &Config) -> Tagger {
        let mut tag_regex_map = HashMap::new();
        for (tag, value) in &config.tags {
            let mut tag_regex_vec = vec![];
            match value {
                Value::Array(regex_array) => {
                    for regex in regex_array {
                        if let Value::String(regex) = regex {
                            tag_regex_vec.push(Regex::new( ("(?i)".to_owned() + regex.as_str()).as_str()).unwrap());
                        }
                    }
                },
                Value::String(regex) => {
                    tag_regex_vec.push(Regex::new(("(?i)".to_owned() + regex.as_str()).as_str()).unwrap());
                },
                _ => {}
            }

            tag_regex_map.insert(tag.clone(), tag_regex_vec);
        }

        Tagger { tag_regex_map }
    }

    /// Try tag a transaction using supplied tags regex table
    pub(crate) fn tag(&self, t: &Transaction) -> Vec<String> {
        let mut tags = vec![];

        for (tag, regex_vec) in &self.tag_regex_map {
            for regex in regex_vec {
                if regex.is_match(t.description.as_str()) {
                    tags.push(tag.clone());
                }
            }
        }

        tags
    }
}

