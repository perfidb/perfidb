use std::error::Error;
use std::fmt;
use roaring::RoaringBitmap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use bytes::BufMut;
use roaring::bitmap::Iter;
use serde::de::Visitor;

/// Create our own roaring bitmap type so we can implement
/// serde::Serialize and Deserialize
pub(crate) struct PerfidbRoaringBitmap(pub(crate) RoaringBitmap);

impl PerfidbRoaringBitmap {
    pub(crate) fn new() -> PerfidbRoaringBitmap {
        PerfidbRoaringBitmap(RoaringBitmap::new())
    }

    pub(crate) fn insert(&mut self, value: u32) -> bool {
        self.0.insert(value)
    }

    pub(crate) fn remove(&mut self, value: u32) -> bool {
        self.0.remove(value)
    }

    pub(crate) fn iter(&self) -> Iter {
        self.0.iter()
    }
}

impl Serialize for PerfidbRoaringBitmap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {

        let mut byte_buffer = Vec::with_capacity(1024).writer();
        self.0.serialize_into(&mut byte_buffer).unwrap();
        serializer.serialize_bytes(byte_buffer.get_ref())
    }
}

struct VisitorPerfidbRoaringBitmap;
impl<'de> Visitor<'de> for VisitorPerfidbRoaringBitmap {
    type Value = PerfidbRoaringBitmap;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("an integer between -2^31 and 2^31")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where E: Error {
        let roaring = RoaringBitmap::deserialize_from(v).unwrap();
        Ok(PerfidbRoaringBitmap(roaring))
    }
}

impl<'de> Deserialize<'de> for PerfidbRoaringBitmap {
    fn deserialize<D>(deserializer: D) -> Result<PerfidbRoaringBitmap, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(VisitorPerfidbRoaringBitmap)
    }
}