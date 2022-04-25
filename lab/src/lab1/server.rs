use async_trait::async_trait;
//use std::sync::mpsc::{Receiver, Sender};
use tonic::{Request, Response, Status};

//use tribbler::err::TribResult;
use tribbler::rpc;
use tribbler::storage;

pub struct StorageServer {
    //pub server: TribStorageServer,
    //pub clock: AtomicU64,
    pub storage: Box<dyn storage::Storage>,
}

#[async_trait]
impl rpc::trib_storage_server::TribStorage for StorageServer {
    async fn get(&self, request: Request<rpc::Key>) -> Result<Response<rpc::Value>, Status> {
        let key = request.into_inner().key;

        match self.storage.get(&key.clone()).await {
            Ok(Some(value)) => return Ok(Response::new(rpc::Value { value: value })),
            Ok(None) => {
                return Ok(Response::new(rpc::Value {
                    value: "".to_string(),
                }))
            }
            _ => return Err(Status::unknown("Get error")),
            //Ok(None) => Err(Status::message("Cannot find the key.")),
            //Err(error) => Err(Status::details(error)),
        };
    }

    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let kv = request.into_inner();

        match self
            .storage
            .set(&storage::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await
        {
            Ok(true) => return Ok(Response::new(rpc::Bool { value: true })),
            Ok(false) => return Ok(Response::new(rpc::Bool { value: false })),
            _ => return Err(Status::unknown("Cannot set the value.")),
        };
    }

    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let p = request.into_inner();

        match self
            .storage
            .keys(&storage::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await
        {
            Ok(list) => {
                return Ok(Response::new(rpc::StringList {
                    list: list.0.clone(),
                }))
            }
            _ => return Err(Status::unknown("Keys error.")),
        };
    }

    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let key = request.into_inner().key;

        match self.storage.list_get(&key.clone()).await {
            Ok(list) => {
                return Ok(Response::new(rpc::StringList {
                    list: list.0.clone(),
                }))
            }
            Err(err) => return Err(Status::unknown("List get error.")),
        };
    }

    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let kv = request.into_inner();

        match self
            .storage
            .list_append(&storage::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await
        {
            Ok(true) => return Ok(Response::new(rpc::Bool { value: true })),
            _ => return Err(Status::unknown("List append error. ")),
        }
    }

    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        let kv = request.into_inner();

        match self
            .storage
            .list_remove(&storage::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await
        {
            Ok(removed) => return Ok(Response::new(rpc::ListRemoveResponse { removed: removed })),
            _ => return Err(Status::unknown("Cannot remove list")),
        };
    }

    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let p = request.into_inner();

        match self
            .storage
            .list_keys(&storage::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await
        {
            Ok(list) => {
                return Ok(Response::new(rpc::StringList {
                    list: list.0.clone(),
                }))
            }
            _ => return Err(Status::unknown("Cannot find list keys")),
        };
    }

    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        let clock = request.into_inner().timestamp;

        match self.storage.clock(clock).await {
            Ok(ret) => return Ok(Response::new(rpc::Clock { timestamp: ret })),
            _ => return Err(Status::unknown("Cannot get clock")),
        };
    }
}
