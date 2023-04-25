use std::collections::{HashMap, HashSet};
use std::ops::BitAnd;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use crate::db::minhash::StringMinHash;
use crate::db::roaring_bitmap::PerfidbRoaringBitmap;
use crate::db::TransactionRecord;

#[derive(Serialize, Deserialize)]
pub(crate) struct SearchIndex {
    token_minhash: StringMinHash,
    /// Map of token hash to set of transactions
    posting_list: HashMap<u32, PerfidbRoaringBitmap>,
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
            let posting: &mut PerfidbRoaringBitmap = self.posting_list.entry(token_hash).or_insert_with(PerfidbRoaringBitmap::new);
            posting.insert(t.id);
        }
    }

    pub(crate) fn delete(&mut self, trans_id: u32, description: &str) {
        for token in description.split_whitespace() {
            let token_hash: Option<u32> = self.token_minhash.lookup_by_string(token);
            if let Some(token_hash) = token_hash {
                self.posting_list.entry(token_hash).and_modify(|bitmap| {
                    bitmap.remove(trans_id);
                });
            }
        }
    }

    pub(crate) fn search(&self, keyword: &str) -> HashSet<u32> {
        let mut maps: Vec<&RoaringBitmap> = vec![];
        for token in keyword.split_whitespace() {
            if let Some(hash) = self.token_minhash.lookup_by_string(token) {
                if let Some(bitmap) = self.posting_list.get(&hash) {
                    maps.push(&bitmap.0);
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
    use crate::db::label_id_vec::LabelIdVec;
    use crate::db::search::SearchIndex;
    use crate::db::TransactionRecord;

    #[test]
    fn test_search_index_serde() {
        let mut search_index = SearchIndex::new();
        let t = TransactionRecord {
            id: 10,
            account: "amex".to_string(),
            date: Default::default(),
            description: "This is a test".to_string(),
            amount: 10.0,
            labels: LabelIdVec::from_vec(vec![1, 3]),
        };
        search_index.index(&t);

        let bytes = bincode::serialize(&search_index).unwrap();
        let search_index: SearchIndex = bincode::deserialize(&bytes).unwrap();
        assert!(search_index.search("this").contains(&10));
    }
}
