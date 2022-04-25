use async_trait::async_trait;
use log::{debug, info};
use std::sync::{Arc, Mutex};

use tribbler::err::{TribResult, TribblerError};
use tribbler::rpc::{
    trib_storage_client::TribStorageClient, Clock, Key, KeyValue as rKeyValue, Pattern as rPattern,
};
use tribbler::storage::{
    KeyList, KeyString, KeyValue as sKeyValue, List, Pattern as sPattern, Storage,
};

pub struct StorageClient {
    //pub client: Mutex<Option<TribStorageClient<tonic::transport::Channel>>>,
    pub addr: String,
}

#[async_trait]
impl KeyString for StorageClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;

        let r = c
            .get(Key {
                key: key.to_string(),
            })
            .await?;
        let empty_str = "".to_string();
        match r.into_inner().value {
            value => {
                if value == empty_str {
                    Ok(None)
                } else {
                    Ok(Some(value))
                }
            }
        }
    }

    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &sKeyValue) -> TribResult<bool> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .set(rKeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &sPattern) -> TribResult<List> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .keys(rPattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;

        match r.into_inner().list {
            vec => Ok(List(vec)),
        }
    }
}

#[async_trait]
impl KeyList for StorageClient {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .list_get(Key {
                key: key.to_string(),
            })
            .await?;
        match r.into_inner().list {
            vec => Ok(List(vec)),
        }
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &sKeyValue) -> TribResult<bool> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .list_append(rKeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        match r.into_inner().value {
            value => Ok(value),
        }
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &sKeyValue) -> TribResult<u32> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .list_remove(rKeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        match r.into_inner().removed {
            removed => Ok(removed),
        }
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &sPattern) -> TribResult<List> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .list_keys(rPattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        match r.into_inner().list {
            vec => Ok(List(vec)),
        }
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        let mut c = TribStorageClient::connect(format!("http://{}", self.addr.clone())).await?;
        let r = c
            .clock(Clock {
                timestamp: at_least.clone(),
            })
            .await?;
        match r.into_inner().timestamp {
            timestamp => Ok(timestamp),
        }
    }
}
