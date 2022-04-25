use std::thread;
use tokio::time;

use super::binstorage::BinStorageClient;
use super::frontend::Frontend;
use super::keeper::Keeper;
use tribbler::{
    config::KeeperConfig, err::TribResult, err::TribblerError, storage::BinStorage, trib::Server,
};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinStorageClient {
        backs: backs.clone(),
        //clock: AtomicU64::new(0),
    }))
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
//#[tokio::main]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    let mut interval = time::interval(time::Duration::from_secs(1));

    let keeper = Keeper {};

    let _ret = match kc.ready {
        Some(ready) => ready.send(true),
        None => Ok(()),
    };

    match kc.shutdown {
        Some(mut shutdown) => loop {
            tokio::select! {
                signal = shutdown.recv() => {
                    match signal {
                        Some(_) => {
                            println!("Keeper killed");
                            return Ok(())
                        },
                        None => (),
                    }
                }
                ret = keeper.sync_clocks(kc.backs.clone()) => {
                    match ret {
                        Ok(_) => (),
                        Err(err) => return Err(err),
                    }
                }
            }
        },
        None => loop {
            match keeper.sync_clocks(kc.backs.clone()).await {
                Ok(_) => (),
                Err(err) => return Err(err),
            }
        },
    }
    // loop {
    //     interval.tick().await;
    //     match keeper.sync_clocks(kc.backs.clone()).await {
    //         Ok(_) => (),
    //         Err(err) => return Err(err),
    //     }
    // }
    Ok(())
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(Frontend {
        binstorage: bin_storage,
        //user_cache: Vec::<String>::new(),
    }))
}
