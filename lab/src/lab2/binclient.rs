use async_trait::async_trait;
use log::{debug, info};
use std::sync::{Arc, Mutex};

use crate::lab1::client::StorageClient;
use tribbler::colon::*;
use tribbler::err::{TribResult, TribblerError};
use tribbler::storage;
use tribbler::storage::{BinStorage, KeyList, KeyString, List, Storage};

pub struct BinClient {
    pub user: String,
    pub storage: Box<dyn storage::Storage>,
}

#[async_trait]
impl KeyString for BinClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(key);

        return self.storage.get(&prefix_key.clone()).await;
    }

    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(&kv.key);

        return self
            .storage
            .set(&storage::KeyValue {
                key: prefix_key,
                value: kv.value.clone(),
            })
            .await;
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &storage::Pattern) -> TribResult<storage::List> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(&p.prefix);

        return self
            .storage
            .keys(&storage::Pattern {
                prefix: prefix_key,
                suffix: p.suffix.clone(),
            })
            .await;
    }
}

#[async_trait]
impl KeyList for BinClient {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<storage::List> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(key);

        return self.storage.list_get(&prefix_key.clone()).await;
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &storage::KeyValue) -> TribResult<bool> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(&kv.key);

        return self
            .storage
            .list_append(&storage::KeyValue {
                key: prefix_key,
                value: kv.value.clone(),
            })
            .await;
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &storage::KeyValue) -> TribResult<u32> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(&kv.key);

        return self
            .storage
            .list_remove(&storage::KeyValue {
                key: prefix_key,
                value: kv.value.clone(),
            })
            .await;
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &storage::Pattern) -> TribResult<List> {
        let mut prefix_key: String = tribbler::colon::escape(self.user.clone());
        prefix_key.push_str("::");
        prefix_key.push_str(&p.prefix);

        return self
            .storage
            .list_keys(&storage::Pattern {
                prefix: prefix_key,
                suffix: p.suffix.clone(),
            })
            .await;
    }
}

#[async_trait]
impl Storage for BinClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        return self.storage.clock(at_least).await;
    }
}
