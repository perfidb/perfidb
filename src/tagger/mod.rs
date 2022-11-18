use std::collections::HashMap;
use regex::Regex;
use toml::Value;
use crate::Config;
use crate::transaction::Transaction;

pub(crate) struct Tagger {
    label_regex_map: HashMap<String, Vec<Regex>>
}

impl Tagger {
    pub(crate) fn new(config: &Config) -> Tagger {
        let mut label_regex_map = HashMap::new();
        for (label, value) in &config.labels {
            let mut label_regex_vec = vec![];
            match value {
                Value::Array(regex_array) => {
                    for regex in regex_array {
                        if let Value::String(regex) = regex {
                            label_regex_vec.push(Regex::new( ("(?i)".to_owned() + regex.as_str()).as_str()).unwrap());
                        }
                    }
                },
                Value::String(regex) => {
                    label_regex_vec.push(Regex::new(("(?i)".to_owned() + regex.as_str()).as_str()).unwrap());
                },
                _ => {}
            }

            label_regex_map.insert(label.clone(), label_regex_vec);
        }

        Tagger { label_regex_map }
    }

    /// Try label a transaction using supplied label regex table
    pub(crate) fn label(&self, t: &Transaction) -> Vec<String> {
        let mut labels = vec![];

        for (label, regex_vec) in &self.label_regex_map {
            for regex in regex_vec {
                if regex.is_match(t.description.as_str()) {
                    labels.push(label.clone());
                }
            }
        }

        labels
    }
}

