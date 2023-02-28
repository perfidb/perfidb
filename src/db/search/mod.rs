use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::{BitAnd};
use roaring::{RoaringBitmap};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde::de::{Error, Visitor};
use crate::db::search::minhash::StringMinHash;
use crate::db::TransactionRecord;

mod minhash;

pub(crate) struct SearchIndex {
    token_minhash: StringMinHash,
    /// Map of token hash to set of transactions
    posting_list: HashMap<u32, RoaringBitmap>,
}

impl Serialize for SearchIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let token_minhash = bincode::serialize(&self.token_minhash).unwrap();
        // TODO: serialise postings list
        serializer.serialize_bytes(&token_minhash)
    }
}

struct VisitorMinhash;

impl<'de> Visitor<'de> for VisitorMinhash {
    type Value = SearchIndex;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between -2^31 and 2^31")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where E: Error {
        let token_minhash: StringMinHash = bincode::deserialize(v).unwrap();
        // TODO: deserialise postings list
        Ok(SearchIndex {
            token_minhash,
            posting_list: HashMap::new()
        })
    }
}

impl<'de> Deserialize<'de> for SearchIndex {
    fn deserialize<D>(deserializer: D) -> Result<SearchIndex, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(VisitorMinhash)
    }
}

impl SearchIndex {
    pub(crate) fn new() -> SearchIndex {
        SearchIndex {
            token_minhash: StringMinHash::new(),
            posting_list: HashMap::new(),
        }
    }

    pub(crate) fn index(&mut self, t: &TransactionRecord) {
        for token in t.description.split_whitespace() {
            let token_hash: u32 = self.token_minhash.put(token);
            let posting: &mut RoaringBitmap = self.posting_list.entry(token_hash).or_insert_with(RoaringBitmap::new);
            posting.insert(t.id);
        }
    }

    pub(crate) fn search(&self, keyword: &str) -> HashSet<u32> {
        let mut maps: Vec<&RoaringBitmap> = vec![];
        for token in keyword.split_whitespace() {
            if let Some(hash) = self.token_minhash.lookup_by_string(token) {
                if let Some(roaring_bitmap) = self.posting_list.get(&hash) {
                    maps.push(roaring_bitmap);
                }
            }
        }

        let mut trans_ids = HashSet::new();
        if !maps.is_empty() {
            let mut intersection = maps[0].clone();
            for map in maps.into_iter().skip(1) {
                intersection = intersection.bitand(map)
            }
            for trans_id in intersection.iter() {
                trans_ids.insert(trans_id);
            }
        }
        trans_ids
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::db::search::minhash::StringMinHash;
    use crate::db::search::SearchIndex;

    #[test]
    fn test_search_index_serde() {
        let mut minhash = StringMinHash::new();
        minhash.put("token");
        let search_index = SearchIndex {
            token_minhash: minhash,
            posting_list: HashMap::new()
        };

        let bytes = bincode::serialize(&search_index).unwrap();
        let search_index: SearchIndex = bincode::deserialize(&bytes).unwrap();
        assert_eq!(1, search_index.token_minhash.lookup_by_string("token").unwrap());
    }
}