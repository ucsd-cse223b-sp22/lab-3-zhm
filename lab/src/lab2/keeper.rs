use async_trait::async_trait;
use log::{debug, info};
use std::{
    cmp,
    sync::{Arc, Mutex},
};
use tokio::time;

use tribbler::err::TribResult;
use tribbler::rpc::{trib_storage_client::TribStorageClient, Clock};

pub struct Keeper {
    //pub clock: &'a mut u64,
//pub backs: Vec<String>, // TODO: store connected clients
}

impl Keeper {
    pub async fn sync_clocks(&self, mut backs: Vec<String>) -> TribResult<()> {
        //*self.clock = *self.clock + 1; // not sure if I can increment self data like this
        let mut clock: u64 = 0;
        for back in backs.iter_mut() {
            let mut c = TribStorageClient::connect(back.clone()).await?; // deal with crashed client
            let r = c.clock(Clock { timestamp: 0 }).await?;

            clock = cmp::max(clock, r.into_inner().timestamp);
        }

        for back in backs.iter_mut() {
            let mut c = TribStorageClient::connect(format!("http://{}", back.clone())).await?; // deal with crashed client
            let r = c
                .clock(Clock {
                    timestamp: clock.clone(),
                })
                .await?;
            // deal with potential error
            match r.into_inner().timestamp {
                timestamp => (),
            }
        }
        time::sleep(time::Duration::from_secs(1)).await;

        Ok(())
    }
}
