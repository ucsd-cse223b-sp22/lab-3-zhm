use crate::lab1::client::StorageClient;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::RwLock;
use tribbler::{
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

const USER_EXISTS_KEY: &str = "_existing_user_#";
const PREFIX: &str = "PREFIX_";
const SUFFIX: &str = "SUFFIX_";

pub struct temp {
    pub storage_clients: Vec<StorageClient>,
    pub live_http_back_addr_idx: Arc<RwLock<Vec<usize>>>,
}

impl temp {
    async fn fetch_all_users(&self, storage: &StorageClient) -> TribResult<Vec<String>> {
        match storage
            .keys(&Pattern {
                prefix: "".to_string(),
                suffix: USER_EXISTS_KEY.to_string(),
            })
            .await
        {
            Ok(List(vec)) => return Ok(vec),
            Err(_) => return Err("Backend crashed when doing migration".into()),
        }
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

    async fn migration_join(
        &self,
        new: usize,
        // successor id in storage_clients
        succ: usize,
    ) -> TribResult<bool> {
        let succ_users = match self.fetch_all_users(&self.storage_clients[succ]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };
        let live_https = self.live_http_back_addr_idx.read().await;
        let live_https_len = live_https.len();

        //let new_node_idx_in_live_https = self.lower_bound_in_list(live_https.deref(), new).await;
        // let succ =  live_https[(new_node_idx_in_live_https+1) % live_https_len as usize];

        // If there is only one live backend => do nothing
        if live_https_len == 1 {
            return Ok(true);
        }

        // If there are only two backends => copy all data from the old one to another
        if live_https_len == 2 {
            for user in succ_users.iter() {
                //tokio::spawn(async move {
                match self
                    .bin_migration(
                        user,
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
        }

        // If there are more than three backends => copy from all the users except the ones between new node and its successor
        for user in succ_users.iter() {
            let idx = self.hash_bin_name_to_backend_idx(user).await?;
            if idx > new && idx <= succ {
                continue;
            }
            //tokio::spawn(async move {
            match self
                .bin_migration(
                    user,
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

        Ok(true)
    }

    async fn migration_crash(
        &self,
        crashed: usize,
        // before predecesssor id
        prev_pred: usize,
        // predecessor id
        pred: usize,
        // successor id
        succ: usize,
        // next successor id
        next_succ: usize,
    ) -> TribResult<bool> {
        // If we just get crashed node's id
        // let live_https = self.live_http_back_addr_idx.read().await;
        // let live_https_len = live_https.len();
        // let crashed_idx_in_live_https = self.lower_bound_in_list(live_https.dedup(), num).await?;
        // let succ = live_https[(crashed_idx_in_live_https + 1) % live_https_len as usize];
        // let succ = live_https[(crashed_idx_in_live_https + 2) % live_https_len as usize];
        // let pred =
        //     live_https[(crashed_idx_in_live_https + live_https_len - 1) % live_https_len as usize];
        // let prev_pred = live_https[(crashed_idx_in_live_https + live_https_len + live_https_len
        //     - 2)
        //     % live_https_len as usize];

        // If there is only one node left => no nothing
        if pred == succ {
            return Ok(true);
        }

        let pred_users = match self.fetch_all_users(&self.storage_clients[pred]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };
        let succ_users = match self.fetch_all_users(&self.storage_clients[succ]).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };

        // Move all the user data who hashed into the range (id of node before predecessor, id of predecessor] to crashed node's successor
        for user in pred_users.iter() {
            let idx = self.hash_bin_name_to_backend_idx(user).await?;
            if idx > prev_pred && idx <= pred {
                match self
                    .bin_migration(
                        user,
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
        for user in succ_users.iter() {
            let idx = self.hash_bin_name_to_backend_idx(user).await?;
            if idx > pred && idx <= crashed {
                match self
                    .bin_migration(
                        user,
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
