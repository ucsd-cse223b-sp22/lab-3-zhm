use async_trait::async_trait;
//use log::{debug, info};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

use super::binclient::BinClient;
use crate::lab1::client::StorageClient;
use tribbler::err::TribResult;
use tribbler::storage::{BinStorage, Storage};

pub struct BinStorageClient {
    pub backs: Vec<String>,
    //pub clock: AtomicU64,
}

#[async_trait]
impl BinStorage for BinStorageClient {
    /// Fetch a [Storage] bin based on the given bin name.
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        let mut hasher = DefaultHasher::new();
        hasher.write(name.as_bytes());
        let hashed_val = hasher.finish() as usize;
        let index: usize = hashed_val % self.backs.len();
        let addr = &self.backs[index];
        let bin_client = Box::new(StorageClient {
            addr: addr.to_string(),
        });
        Ok(Box::new(BinClient {
            user: name.to_string(),
            storage: bin_client,
        }))
    }
}
