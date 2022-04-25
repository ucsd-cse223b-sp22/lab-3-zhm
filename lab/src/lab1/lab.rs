use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
//use tokio::sync::mpsc::{Receiver, Sender};
//use log::{debug, info};
use tonic::transport::Server;

use super::client::StorageClient;
use super::server::StorageServer;
use tribbler::err::TribblerError;
use tribbler::rpc::trib_storage_client::TribStorageClient;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let addr = match config.addr.clone().to_socket_addrs() {
        Ok(mut addr_iter) => match addr_iter.next() {
            Some(addr) => addr,
            None => {
                return Err(Box::new(TribblerError::Unknown(
                    "Canoot parse address".to_string(),
                )))
            }
        },
        Err(err) => return Err(Box::new(err)),
    };

    let storage = config.storage;

    let storage_server = StorageServer { storage: storage };

    let service = TribStorageServer::new(storage_server);

    match config.ready {
        Some(ready) => ready.send(true),
        None => Ok(()),
    };

    match config.shutdown {
        Some(mut shutdown) => {
            Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    shutdown.recv().await;
                })
                .await?
        }
        None => Server::builder().add_service(service).serve(addr).await?,
    }

    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    Ok(Box::new(StorageClient {
        //client: Mutex::new(None),
        addr: addr.to_string(),
    }))
}
