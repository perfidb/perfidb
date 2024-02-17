use std::collections::{BTreeSet};
use std::fs;
use std::io::Write;

use byteorder::{LittleEndian, WriteBytesExt};
use serde::{Deserialize, Serialize};
use crate::db::{Database, Metadata, PERFIDB_VERSION};

#[derive(Serialize, Deserialize)]
pub(crate) struct ShadowDatabase {
    pub(crate) database: Database,

    pub(crate) files_root_path: String,

    /// Files already imported. Each entry is a relative path to the root path.
    pub(crate) imported_files: BTreeSet<String>,
}

impl ShadowDatabase {
    /// Save db content to disk
    pub(crate) fn save(&self) {
        // Create metadata using current binary version
        let metadata = Metadata { version: PERFIDB_VERSION.to_string() };
        let metadata_encoded: Vec<u8> = bincode::serialize(&metadata).unwrap();
        let metadata_length = metadata_encoded.len();
        assert!(metadata_length <= (u16::MAX - 2) as usize);

        let encoded: Vec<u8> = bincode::serialize(&self).unwrap();

        // Use first 1024 bytes to store metadata
        let mut file = fs::File::create(self.database.file_path.as_ref().unwrap()).unwrap();
        // Using first 2 bytes to write metadata length
        file.write_u16::<LittleEndian>(metadata_length as u16).unwrap();
        // Write metadata
        file.write_all(&metadata_encoded).unwrap();
        let remaining_header_bytes = 1024 - 2 - metadata_length;
        // Write 0s for remaining bytes to fill up the first 1024 bytes.
        file.write_all(&vec![0; remaining_header_bytes]).unwrap();

        file.write_all(&encoded).expect("Unable to write to database file");
        file.flush().unwrap();
    }
}