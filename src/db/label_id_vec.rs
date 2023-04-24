use std::ops::Deref;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct LabelIdVec(Vec<u32>);

impl LabelIdVec {
    pub(crate) fn empty() -> LabelIdVec {
        LabelIdVec(vec![])
    }

    pub(crate) fn from_vec(vec: Vec<u32>) -> LabelIdVec {
        LabelIdVec(vec)
    }

    pub(crate) fn add(&mut self, label_id: u32) -> bool {
        match self.0.iter().position(|&item| item == label_id) {
            Some(_index) => false,
            None => {
                self.0.push(label_id);
                true
            }
        }
    }

    pub(crate) fn remove(&mut self, label_id: u32) -> bool {
        match self.0.iter().position(|&item| item == label_id) {
            Some(index) => {
                self.0.remove(index);
                true
            },
            None => false
        }
    }
}

impl Deref for LabelIdVec {
    type Target = Vec<u32>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}