use std::collections::HashMap;
use std::hash::Hash;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct StringMinHash {
    string_to_id: HashMap<String, u32>,
    id_to_string: HashMap<u32, String>,
    next_id: u32,
}

impl StringMinHash {
    pub(crate) fn new() -> StringMinHash {
        StringMinHash {
            string_to_id: HashMap::new(),
            id_to_string: HashMap::new(),
            next_id: 1,
        }
    }

    pub(crate) fn put<S>(&mut self, str: S) -> u32 where S: Into<String> + Hash {
        let string = str.into().to_lowercase();

        match self.string_to_id.get(&string) {
            Some(hash) => *hash,
            None => {
                let hash = self.next_id;
                self.next_id += 1;
                self.string_to_id.insert(string.clone(), hash);
                self.id_to_string.insert(hash, string.clone());
                hash
            }
        }
    }

    pub(crate) fn lookup_by_hash(&self, hash: u32) -> Option<&String> {
        self.id_to_string.get(&hash)
    }

    pub(crate) fn lookup_by_string<S>(&self, str: S) -> Option<u32> where S: Into<String> {
        self.string_to_id.get(&str.into().to_lowercase()).copied()
    }
}