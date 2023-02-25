use std::collections::{HashMap, HashSet};
use std::ops::{BitAnd, BitOr};
use roaring::{RoaringBitmap};
use crate::db::search::minhash::StringMinHash;
use crate::db::TransactionRecord;

mod minhash;

pub(crate) struct SearchIndex {
    token_minhash: StringMinHash,
    /// Map of token hash to set of transactions
    posting_list: HashMap<u32, RoaringBitmap>,
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