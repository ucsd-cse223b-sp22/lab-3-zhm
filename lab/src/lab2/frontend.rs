#![allow(dead_code)]
use async_trait::async_trait;
//use log::{debug, info};
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use std::{
    cmp::{min, Ordering},
    collections::HashSet,
    sync::{Arc, RwLock},
    time::SystemTime,
};

use tribbler::err::{TribResult, TribblerError};
use tribbler::storage::{BinStorage, KeyValue, List};
use tribbler::trib::{
    is_valid_username, Server, Trib, MAX_FOLLOWING, MAX_TRIB_FETCH, MAX_TRIB_LEN, MIN_LIST_USER,
};

#[allow(dead_code)]
pub const USER_NUM: &str = "user_number";

#[allow(dead_code)]
pub const FOLLOW_LOG: &str = "follow_log";

#[allow(dead_code)]
pub const GLOBAL_USERS: &str = "global_users";

#[allow(dead_code)]
pub const SIGNED_UP: &str = "signed_up";

#[allow(dead_code)]
pub const MAIN: &str = "main";

#[allow(dead_code)]
pub const FOLLOW_NUM: &str = "following_num";

#[allow(dead_code)]
pub const TRIBS: &str = "tribs";

#[allow(dead_code)]
pub const USER_CACHE: &str = "user_cache";

pub struct Frontend {
    pub binstorage: Box<dyn BinStorage>,
    //pub user_cache: Vec<String>,
    //pub clock: u64,
}

impl Frontend {
    async fn register_to_main(&self, user: &str) -> TribResult<usize> {
        let main_bin = match self.binstorage.bin(MAIN).await {
            Ok(storage) => storage,
            Err(err) => return Err(err), // TODO: deal with this for fault tolerance
        };

        let mut global_users = match main_bin.list_get(GLOBAL_USERS).await {
            Ok(List(vec)) => vec,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut user_num: usize = global_users.len();

        if user_num < MIN_LIST_USER {
            match main_bin
                .list_append(&KeyValue {
                    key: GLOBAL_USERS.to_string(),
                    value: user.to_string(),
                })
                .await
            {
                Ok(_) => (),
                Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
            }
        };

        user_num = user_num + 1;

        Ok(user_num)
    }
    pub fn contains_in_vec(&self, key: String, vec: Vec<String>) -> bool {
        let vec_iter = vec.iter();
        for item in vec_iter {
            if &key == item {
                return true;
            }
        }
        return false;
    }
    // async fn cache_users(&mut self, user: &str) -> TribResult<()> {
    //     let main_bin = match self.binstorage.bin(MAIN).await {
    //         Ok(storage) => storage,
    //         Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
    //     };

    //     let mut global_users = match main_bin.list_get("global_users").await {
    //         Ok(List(vec)) => vec,
    //         Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
    //     };

    //     // let user_bin = match self.binstorage.bin(user).await {
    //     //     Ok(storage) => storage,
    //     //     Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
    //     // }

    //     let vec_iter = global_users.iter_mut();

    //     for item in vec_iter {
    //         // user_bin.list_append(&KeyValue{
    //         //     key: USER_CACHE,
    //         //     value: item.to_string(),
    //         // });
    //         self.user_cache.push(item.to_string());
    //     }

    //     Ok(())
    // }
}

#[async_trait]
impl Server for Frontend {
    /// Creates a user.
    /// Returns error when the username is invalid; //
    /// returns error when the user already exists.
    /// Concurrent sign ups on the same user might both succeed with no error.

    async fn sign_up(&self, user: &str) -> TribResult<()> {
        if !is_valid_username(user) {
            return Err(Box::new(TribblerError::InvalidUsername(user.to_string())));
        };

        let user_bin = match self.binstorage.bin(user).await {
            Ok(storage) => storage,
            Err(err) => return Err(err),
        };

        match user_bin.get(SIGNED_UP).await {
            Ok(Some(user)) => return Err(Box::new(TribblerError::UsernameTaken(user.to_string()))),
            Ok(None) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        // initialize user

        // init signed_up
        match user_bin
            .set(&KeyValue {
                key: SIGNED_UP.to_string(),
                value: user.to_string(),
            })
            .await
        {
            Ok(true) => (),
            Ok(false) => {
                return Err(Box::new(TribblerError::Unknown(
                    "User cannot signed up".to_string(),
                )))
            }
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        //init follow_log
        match user_bin
            .list_append(&KeyValue {
                key: FOLLOW_LOG.to_string(),
                value: "start".to_string(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        // init follow num
        match user_bin
            .set(&KeyValue {
                key: FOLLOW_NUM.to_string(),
                value: "0".to_string(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut user_num: usize = 0;

        // check if need to register to global users
        match user_bin.get(USER_NUM).await {
            Ok(Some(num_str)) => {
                user_num = match num_str.parse() {
                    Ok(num) => num,
                    Err(err) => {
                        return Err(Box::new(TribblerError::Unknown(
                            "String to integer parsing error.".to_string(),
                        )))
                    }
                };
                if user_num < MIN_LIST_USER {
                    user_num = match self.register_to_main(&user.clone()).await {
                        Ok(num) => num,
                        Err(err) => return Err(err),
                    };
                }
            }
            Ok(None) => {
                user_num = match self.register_to_main(&user.clone()).await {
                    Ok(num) => num,
                    Err(err) => return Err(err),
                }
            }
            Err(err) => (),
        }

        user_num = user_num + 1;
        if user_num >= MIN_LIST_USER {
            match user_bin
                .set(&KeyValue {
                    key: USER_NUM.to_string(),
                    value: user_num.to_string(),
                })
                .await
            {
                Ok(_) => (),
                Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
            };
            // for cache user list in frontend
            // let user_cache = match user_bin.list_get(USER_CACHE) {
            //     Ok(List(vec)) => vec,
            //     Err(err) => return Err(Box::new(TribblerError::Unknown(err.to_string()))),
            // };
            // if user_cache.len() == 0 {
            //     match self.cache_users(user).await {
            //         Ok(_) => (),
            //         Err(err) => {
            //             return Err(Box::new(TribblerError::Unknown(err.to_string())))
            //         }
            //     };
            // }
        }

        Ok(())
    }

    /// List 20 registered users.  When there are less than 20 users that
    /// signed up the service, all of them needs to be listed.  When there
    /// are more than 20 users that signed up the service, an arbitrary set
    /// of at lest 20 of them needs to be listed.
    /// The result should be sorted in alphabetical order.
    async fn list_users(&self) -> TribResult<Vec<String>> {
        // get user list from frontend cache
        // let user_cache = match self.user_cache.read() {
        //     Ok(user_cache) => user_cache,
        //     Err(err) => return Err(Box::new(TribblerError::Unknown(err.to_string()))),
        // };

        // if user_cache.len() > 0 {
        //     return Ok(user_cache.to_vec());
        // };

        let user_bin = match self.binstorage.bin(MAIN).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut global_users = match user_bin.list_get(GLOBAL_USERS).await {
            Ok(List(vec)) => vec,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        global_users.sort();
        let sorted = global_users[..min(MIN_LIST_USER, global_users.len())].to_vec();
        let res: Vec<String> = sorted
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        Ok(res)
    }

    /// Post a tribble.  The clock is the maximum clock value this user has
    /// seen so far by reading tribbles or clock sync.
    /// Returns error when who does not exist;
    /// returns error when post is too long.
    async fn post(&self, who: &str, post: &str, clock: u64) -> TribResult<()> {
        //todo!();
        if post.len() > MAX_TRIB_LEN {
            return Err(Box::new(TribblerError::TribTooLong));
        }
        let who_bin = match self.binstorage.bin(who).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        match who_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        let trib_clock = match who_bin.clock(clock).await {
            Ok(timestamp) => timestamp,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let new_trib = Trib {
            user: who.to_string(),
            message: post.to_string(),
            time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs(),
            clock: trib_clock,
        };

        let new_trib_str = match serde_json::to_string(&new_trib) {
            Ok(new_trib_str) => new_trib_str,
            Err(err) => return Err(Box::new(TribblerError::Unknown(err.to_string()))),
        };

        match who_bin
            .list_append(&KeyValue {
                key: TRIBS.to_string(),
                value: new_trib_str,
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        Ok(())
    }

    /// List the tribs that a particular user posted.
    /// Returns error when user has not signed up.
    async fn tribs(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        let user_bin = match self.binstorage.bin(user).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        // check if user signed
        match user_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let tribs = match user_bin.list_get(TRIBS).await {
            Ok(List(tribs)) => tribs,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut trib_vec = Vec::new();
        if tribs.len() == 0 {
            return Ok(trib_vec);
        };

        let mut index: usize = 0;
        let mut tribs_len: usize = tribs.len();
        while index < tribs_len {
            let trib: Trib = match serde_json::from_str(&tribs[index]) {
                Ok(trib) => trib,
                Err(err) => return Err(Box::new(TribblerError::Unknown(err.to_string()))),
            };
            let trib_arc = Arc::new(trib);
            trib_vec.push(trib_arc.clone());
            index = index + 1;
        }

        trib_vec.sort_by(|a, b| {
            a.clock
                .cmp(&b.clock)
                .then_with(|| a.time.cmp(&b.time))
                .then_with(|| a.user.cmp(&b.user))
                .then_with(|| a.message.cmp(&b.message))
        });

        let ntrib = trib_vec.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        let trimmed_tribs = trib_vec[start..].to_vec();

        // garbage collection
        let mut index: usize = 0;
        while index < start {
            let trib_str = match serde_json::to_string(&trib_vec[index]) {
                Ok(trib_str) => trib_str,
                Err(err) => return Err(Box::new(TribblerError::Unknown(err.to_string()))),
            };
            match user_bin
                .list_remove(&KeyValue {
                    key: TRIBS.to_string(),
                    value: trib_str,
                })
                .await
            {
                Ok(_) => (),
                Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
            };
            index = index + 1;
        }

        return Ok(trimmed_tribs);
    }

    /// Follow someone's timeline.
    /// Returns error when who == whom;
    /// returns error when who is already following whom;
    /// returns error when who is trying to following
    /// more than trib.MaxFollowing users. // following num
    /// returns error when who or whom has not signed up.
    /// Concurrent follows might both succeed without error.
    /// The count of following users might exceed trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn follow(&self, who: &str, whom: &str) -> TribResult<()> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        };
        let who_bin = match self.binstorage.bin(who).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        match who_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        let whom_bin = match self.binstorage.bin(whom).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        match whom_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        // add to follow log
        let curr_clock = match who_bin.clock(0).await {
            Ok(timestamp) => timestamp,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let curr_record = format!("{}::follow::{}", curr_clock.to_string(), whom.to_string());
        match who_bin
            .list_append(&KeyValue {
                key: FOLLOW_LOG.to_string(),
                value: curr_record.clone(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let log = match who_bin.list_get(FOLLOW_LOG).await {
            Ok(List(log)) => log,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut follow_state: bool = false;
        let log_iter = log.iter();
        for record in log_iter {
            if record.to_string() == curr_record {
                if follow_state {
                    return Err(Box::new(TribblerError::AlreadyFollowing(
                        who.to_string(),
                        whom.to_string(),
                    )));
                    // match who_bin
                    //     .list_remove(&KeyValue {
                    //         key: FOLLOW_LOG.to_string(),
                    //         value: curr_record,
                    //     })
                    //     .await
                    // {
                    //     Ok(_) => {
                    //         return Err(Box::new(TribblerError::AlreadyFollowing(
                    //             who.to_string(),
                    //             whom.to_string(),
                    //         )));
                    //     }
                    //     Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
                    // };
                };
                break;
            };

            // currently not deal with other errors, eq. other multiple follow and unfollow, since other frontend might
            // be dealing with it => not sure should I deal with it here
            match record.find(&format!("::follow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == false {
                        follow_state = true;
                    }
                }
                None => (),
            }
            match record.find(&format!("::unfollow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == true {
                        follow_state = false;
                    }
                }
                None => (),
            }
        }

        let mut following_num: usize = match who_bin.get(FOLLOW_NUM).await {
            Ok(Some(num_str)) => match num_str.parse() {
                Ok(num) => num,
                Err(err) => {
                    return Err(Box::new(TribblerError::Unknown(
                        "Parse string to integer error".to_string(),
                    )))
                }
            },
            Ok(None) => {
                return Err(Box::new(TribblerError::Unknown(
                    "Following num initialize problem".to_string(),
                )))
            }
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        following_num = following_num + 1;

        if following_num > MAX_FOLLOWING {
            match who_bin
                .list_remove(&KeyValue {
                    key: FOLLOW_LOG.to_string(),
                    value: curr_record,
                })
                .await
            {
                Ok(_) => return Err(Box::new(TribblerError::FollowingTooMany)),
                Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
            }
            //return Err(Box::new(TribblerError::FollowingTooMany));
        }

        match who_bin
            .set(&KeyValue {
                key: FOLLOW_NUM.to_string(),
                value: following_num.to_string(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        Ok(())
    }

    /// Unfollow someone's timeline.
    /// Returns error when who == whom.
    /// returns error when who is not following whom;
    /// returns error when who or whom has not signed up.
    async fn unfollow(&self, who: &str, whom: &str) -> TribResult<()> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        };
        let who_bin = match self.binstorage.bin(who).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        //check who signed up
        match who_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let whom_bin = match self.binstorage.bin(whom).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        //check whom signed up
        match whom_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let curr_clock = match who_bin.clock(0).await {
            Ok(timestamp) => timestamp,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let curr_record = format!("{}::unfollow::{}", curr_clock.to_string(), whom.to_string());
        match who_bin
            .list_append(&KeyValue {
                key: FOLLOW_LOG.to_string(),
                value: curr_record.clone(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let log = match who_bin.list_get(FOLLOW_LOG).await {
            Ok(List(log)) => log,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut follow_state: bool = false;
        let log_iter = log.iter();
        for record in log_iter {
            //println!("{}", record.to_string());
            if record.to_string() == curr_record {
                if follow_state == false {
                    return Err(Box::new(TribblerError::NotFollowing(
                        who.to_string(),
                        whom.to_string(),
                    )));
                    // match who_bin
                    //     .list_remove(&KeyValue {
                    //         key: FOLLOW_LOG.to_string(),
                    //         value: curr_record,
                    //     })
                    //     .await
                    // {
                    //     Ok(_) => {
                    //         return Err(Box::new(TribblerError::NotFollowing(
                    //             who.to_string(),
                    //             whom.to_string(),
                    //         )));
                    //     }
                    //     Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
                    // };
                };
                //println!("{}", follow_state.to_string());
                break;
            };

            // currently not deal with other errors, eq. other multiple follow and unfollow, since other frontend might
            // be dealing with it => not sure should I deal with it here
            match record.find(&format!("::follow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == false {
                        follow_state = true;
                    }
                }
                None => (),
            }
            match record.find(&format!("::unfollow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == true {
                        follow_state = false;
                    }
                }
                None => (),
            }
        }

        // Connection might be lost here: think about it
        let mut following_num: usize = match who_bin.get(FOLLOW_NUM).await {
            Ok(Some(num_str)) => match num_str.parse() {
                Ok(num) => num,
                Err(err) => {
                    return Err(Box::new(TribblerError::Unknown(
                        "Parse string to integer error".to_string(),
                    )))
                }
            },
            Ok(None) => {
                return Err(Box::new(TribblerError::Unknown(
                    "Following num initialize problem".to_string(),
                )))
            }
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        following_num = following_num - 1;

        match who_bin
            .set(&KeyValue {
                key: FOLLOW_NUM.to_string(),
                value: following_num.to_string(),
            })
            .await
        {
            Ok(_) => (),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        }

        Ok(())
    }

    /// Returns true when who following whom.
    /// Returns error when who == whom.
    /// Returns error when who or whom has not signed up.
    async fn is_following(&self, who: &str, whom: &str) -> TribResult<bool> {
        if who == whom {
            return Err(Box::new(TribblerError::WhoWhom(who.to_string())));
        }
        let who_bin = match self.binstorage.bin(who).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        //check who signed up
        match who_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let whom_bin = match self.binstorage.bin(whom).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        // check whom signed up
        match whom_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(whom.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        // let curr_clock = match who_bin.clock(0).await {
        //     Ok(timestamp) => timestamp,
        //     Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        // };

        let log = match who_bin.list_get(FOLLOW_LOG).await {
            Ok(List(log)) => log,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut follow_state: bool = false;
        let log_iter = log.iter();
        for record in log_iter {
            match record.find(&format!("::follow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == false {
                        follow_state = true;
                    }
                }
                None => (),
            }
            match record.find(&format!("::unfollow::{}", whom.to_string())) {
                Some(_) => {
                    if follow_state == true {
                        follow_state = false;
                    }
                }
                None => (),
            }
        }

        return Ok(follow_state);
    }

    /// Returns the list of following users.
    /// Returns error when who has not signed up.
    /// The list have users more than trib.MaxFollowing=2000,
    /// if and only if the 2000'th user is generated by concurrent Follow()
    /// calls.
    async fn following(&self, who: &str) -> TribResult<Vec<String>> {
        let who_bin = match self.binstorage.bin(who).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        //check who signed up
        match who_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(who.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let mut following_set = HashSet::new();
        let mut log = match who_bin.list_get(FOLLOW_LOG).await {
            Ok(List(log)) => log,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        let log_iter = log.iter_mut();
        for record in log_iter {
            match record.find("::follow::") {
                Some(index) => {
                    let whom = &record[index..];
                    let name_slice: Vec<&str> = whom.split("::").collect();
                    following_set.insert(name_slice[2]);
                }
                None => (),
            }
            match record.find("::unfollow::") {
                Some(index) => {
                    let whom = &record[index..];
                    let name_slice: Vec<&str> = whom.split("::").collect();
                    following_set.remove(name_slice[2]);
                }
                None => (),
            }
        }

        let mut following_vec: Vec<String> = following_set.into_iter().map(String::from).collect();
        Ok(following_vec)
    }

    /// List the tribs of someone's following users (including himself).
    /// Returns error when user has not signed up.
    async fn home(&self, user: &str) -> TribResult<Vec<Arc<Trib>>> {
        //todo!();

        let user_bin = match self.binstorage.bin(user).await {
            Ok(storage) => storage,
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };
        //check who signed up
        match user_bin.get(SIGNED_UP).await {
            Ok(Some(_)) => (),
            Ok(None) => return Err(Box::new(TribblerError::UserDoesNotExist(user.to_string()))),
            Err(err) => return Err(Box::new(TribblerError::RpcError(err.to_string()))),
        };

        let following = match self.following(user).await {
            Ok(vec) => vec,
            Err(err) => return Err(err),
        };

        let user_tribs = match self.tribs(user).await {
            Ok(tribs) => tribs,
            Err(err) => return Err(err),
        };

        let mut home: Vec<Arc<Trib>> = vec![];
        home.append(&mut user_tribs.clone());
        for who in following.iter() {
            let follow_trib = match self.tribs(who).await {
                Ok(tribs) => tribs,
                Err(err) => return Err(err),
            };
            home.append(&mut follow_trib.clone());
        }
        home.sort_by(|a, b| {
            a.clock
                .cmp(&b.clock)
                .then_with(|| a.time.cmp(&b.time))
                .then_with(|| a.user.cmp(&b.user))
                .then_with(|| a.message.cmp(&b.message))
        });
        let ntrib = home.len();
        let start = match ntrib.cmp(&MAX_TRIB_FETCH) {
            Ordering::Greater => ntrib - MAX_TRIB_FETCH,
            _ => 0,
        };
        Ok(home[start..].to_vec())
    }
}
