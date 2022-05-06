use crate::lab1::client::StorageClient;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::env::consts::DLL_SUFFIX;
use std::hash::{Hash, Hasher};
use std::iter::successors;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tribbler::{
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

// const USER_EXISTS_KEY: &str = "_existing_user_#";
const PREFIX: &str = "PREFIX_";
const SUFFIX: &str = "SUFFIX_";

pub struct temp {
    pub storage_clients: Vec<StorageClient>,
    pub live_http_back_addr_idx: Arc<RwLock<Vec<usize>>>,
}

// TODO:
impl temp {
    async fn fetch_all_bins(&self, storage: &StorageClient) -> TribResult<Vec<String>> {
        // fetch both keys and list_keys => do it seperately
        // strips prefix
        let all_keys = match storage
            .keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await
        {
            Ok(List(vec)) => vec,
            Err(_) => return Err("Backend crashed when doing migration".into()),
        };

        let mut records = HashSet::<String>::new();

        for key in all_keys.iter() {
            // key format, bin_name::key
            // split string and try to get only the bin_name part
            let key_split: Vec<&str> = key.split("::").collect();
            records.insert(key_split[0].to_string());
        }

        let all_list_keys = match storage
            .list_keys(&Pattern {
                prefix: "".to_string(),
                suffix: "".to_string(),
            })
            .await
        {
            Ok(List(vec)) => vec,
            Err(_) => return Err("Backend crashed when gdoing migratino".into()),
        };

        // PREFFIX & SUFFIX as the same length
        // truncated type part from key
        let prefix_len = PREFIX.len();

        for key in all_list_keys.iter() {
            let untyped_key = &key[prefix_len..];
            let key_split: Vec<&str> = untyped_key.split("::").collect();
            records.insert(key_split[0].to_string());
        }

        let keys_vec: Vec<String> = records.into_iter().collect();

        Ok(keys_vec)
    }

    // Find the lower_bound
    fn lower_bound_in_list(&self, live_https: &Vec<usize>, num: usize) -> usize {
        for (i, https) in live_https.iter().enumerate() {
            if https >= &num {
                return i;
            }
        }

        return 0;
    }

    async fn hash_bin_name_to_backend_idx(&self, bin_name: &str) -> TribResult<usize> {
        let mut hasher = DefaultHasher::new();
        bin_name.hash(&mut hasher);
        let live_https = self.live_http_back_addr_idx.read().await;

        let storage_clients_len = self.storage_clients.len();
        let primary_hash = hasher.finish() % storage_clients_len as u64;

        let primary_idx = self.lower_bound_in_list(live_https.deref(), primary_hash as usize);

        Ok(live_https[primary_idx])
    }

    async fn migration_join(&self, new: usize) -> TribResult<bool> {
        let live_https = self.live_http_back_addr_idx.read().await;
        let live_https_len = live_https.len();

        let new_node_idx_in_live_https = self.lower_bound_in_list(live_https.deref(), new);
        let succ = live_https[(new_node_idx_in_live_https + 1) % live_https_len as usize];

        let succ_bins = match self.fetch_all_bins(&self.storage_clients[succ]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };

        // If there is only one live backend => do nothing
        if live_https_len == 1 {
            return Ok(true);
        }

        // If there are only two backends => copy all data from the old one to another
        if live_https_len == 2 {
            for bin in succ_bins.iter() {
                //tokio::spawn(async move {
                match self
                    .bin_migration(
                        &bin,
                        &self.storage_clients[succ],
                        &self.storage_clients[new],
                    )
                    .await
                {
                    Ok(_) => (),
                    Err(err) => return Err(err),
                }
                //});
            }
            return Ok(true);
        }

        // If there are more than three backends => copy from all the users except the ones between new node and its successor
        for bin in succ_bins.iter() {
            let idx = self.hash_bin_name_to_backend_idx(&bin).await?;
            // Add wrap around check
            if self.check_in_bound_wrap_around(new, succ, idx) {
                continue;
            }
            //tokio::spawn(async move {
            match self
                .bin_migration(bin, &self.storage_clients[succ], &self.storage_clients[new])
                .await
            {
                Ok(_) => (),
                Err(err) => return Err(err),
            }
            //});
        }

        Ok(true)
    }

    fn check_in_bound_wrap_around(&self, left: usize, right: usize, target: usize) -> bool {
        if left > right {
            if target >= right || target < left {
                return true;
            } else {
                return false;
            }
        } else {
            if target > left && target <= right {
                return true;
            } else {
                return true;
            }
        }
    }

    async fn migration_crash(&self, crashed: usize) -> TribResult<bool> {
        let live_https = self.live_http_back_addr_idx.read().await;
        let live_https_len = live_https.len();
        let succ_idx_in_live_https = self.lower_bound_in_list(live_https.deref(), crashed);
        let succ = live_https[succ_idx_in_live_https];
        let next_succ = live_https[(succ_idx_in_live_https + 1) % live_https_len as usize];
        let pred =
            live_https[(succ_idx_in_live_https + live_https_len - 1) % live_https_len as usize];
        let prev_pred = live_https[(succ_idx_in_live_https + live_https_len + live_https_len - 2)
            % live_https_len as usize];

        // If there is only one node left => no nothing
        if pred == succ {
            return Ok(true);
        }

        let pred_bins = match self.fetch_all_bins(&self.storage_clients[pred]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };
        let succ_bins = match self.fetch_all_bins(&self.storage_clients[succ]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };

        // Move all the user data who hashed into the range (id of node before predecessor, id of predecessor] to crashed node's successor
        for bin in pred_bins.iter() {
            let idx = self.hash_bin_name_to_backend_idx(bin).await?;
            if self.check_in_bound_wrap_around(prev_pred, pred, idx) {
                match self
                    .bin_migration(
                        bin,
                        &self.storage_clients[pred],
                        &self.storage_clients[succ],
                    )
                    .await
                {
                    Ok(_) => (),
                    Err(err) => return Err(err),
                }
            }
        }

        // Move all the user data who hashed into the range (id of predecessor, crashed_node] to the node succeed crashed node's successor
        for bin in succ_bins.iter() {
            let idx = self.hash_bin_name_to_backend_idx(bin).await?;
            if self.check_in_bound_wrap_around(pred, crashed, idx) {
                match self
                    .bin_migration(
                        bin,
                        &self.storage_clients[succ],
                        &self.storage_clients[next_succ],
                    )
                    .await
                {
                    Ok(_) => (),
                    Err(err) => return Err(err),
                }
            }
        }

        Ok(true)
    }

    async fn bin_migration(
        &self,
        bin_name: &str,
        from: &StorageClient,
        to: &StorageClient,
    ) -> TribResult<bool> {
        // Copying KeyValue pair
        // How keeper record this part? It seems okay to redo all set operation
        // However, it seems not that efficient
        let keys = from
            .keys(&Pattern {
                prefix: bin_name.clone().to_string(),
                suffix: "".to_string(),
            })
            .await?
            .0;

        // Get value from storage and append it to backend
        //tokio::spawn(async move {
        for key in keys.iter() {
            let value = match from.get(key).await {
                Ok(Some(val)) => val,
                Ok(None) => continue,
                Err(_) => return Err("Backend crashed when doing migration".into()),
            };
            match to
                .set(&KeyValue {
                    key: key.clone(),
                    value,
                })
                .await
            {
                Ok(_) => (),
                Err(_) => return Err("Backend crashed when doing migration".into()),
            }
        }
        //});

        // Copying list
        // Should be prefix_username::trib, prefix_username::follow_log
        // Keeper sends acknowledgement when receiving both prefix and suffix log
        let prefix_list_keys = from
            .list_keys(&Pattern {
                prefix: format!("{}{}::", PREFIX, &bin_name.clone().to_string()),
                suffix: "".to_string(),
            })
            .await?
            .0;

        for key in prefix_list_keys.iter() {
            //tokio::spawn(async move {
            let list = match from.list_get(key).await {
                Ok(List(vec)) => vec,
                Err(_) => return Err("Backend crashed when doing migration".into()),
            };
            for item in list.iter() {
                match to
                    .list_append(&KeyValue {
                        key: key.clone(),
                        value: item.clone(),
                    })
                    .await
                {
                    Ok(_) => (),
                    Err(_) => return Err("Backend crashed when doing migration".into()),
                }
            }

            // Keeper sends keys to other keeper to acknowledge they finished copying this list
            // send_key(key);
            //})
        }

        // Should be suffix_username::trib, suffix_username::follow_log
        let suffix_list_keys = from
            .list_keys(&Pattern {
                prefix: format!("{}{}::", SUFFIX, &bin_name.clone().to_string()),
                suffix: "".to_string(),
            })
            .await?
            .0;

        for key in suffix_list_keys.iter() {
            //tokio::spawn(async move {
            let list = match from.list_get(key).await {
                Ok(List(vec)) => vec,
                Err(_) => return Err("Backend crashed when doing migration".into()),
            };
            for item in list.iter() {
                match to
                    .list_append(&KeyValue {
                        key: key.clone(),
                        value: item.clone(),
                    })
                    .await
                {
                    Ok(_) => (),
                    Err(_) => return Err("Backend crashed when doing migration".into()),
                }
            }

            // Keeper sends keys to other keeper to acknowledge they finished copying this list
            // send_key(key);
            //})
        }

        Ok(true)
    }
}
