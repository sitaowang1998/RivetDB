use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tonic::{Response, Status};

use crate::node::RivetNode;
use crate::storage::{StorageEngine, StorageError};
use crate::transaction::{CommitReceipt, TransactionContext, TransactionManager};
use crate::types::TxnId;

use super::service::rivet_kv_server::RivetKv;
use super::service::{
    AbortRequest, AbortResponse, AbortSuccess, BeginTransactionRequest, BeginTransactionResponse,
    CommitRequest, CommitResponse, CommitSuccess, GetRequest, GetResponse, GetSuccess, PutRequest,
    PutResponse, PutSuccess, abort_response, commit_response, get_response, put_response,
};

/// gRPC service that bridges the transaction manager with network clients.
pub struct RivetKvService<S: StorageEngine> {
    node: Arc<RivetNode<S>>,
    in_flight: Mutex<HashMap<TxnId, TransactionContext>>,
}

impl<S: StorageEngine> RivetKvService<S> {
    const INVALID_TXN_ID_MESSAGE: &'static str = "invalid transaction id";

    pub fn new(node: Arc<RivetNode<S>>) -> Self {
        Self {
            node,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    fn manager(&self) -> &TransactionManager<S> {
        self.node.transaction_manager()
    }

    fn parse_txn_id(raw: &str) -> Result<TxnId, ()> {
        raw.parse::<TxnId>().map_err(|_| ())
    }

    async fn store_txn(&self, txn: TransactionContext) {
        let mut guard = self.in_flight.lock().await;
        guard.insert(txn.id().clone(), txn);
    }

    async fn take_txn(&self, txn_id: &TxnId) -> Option<TransactionContext> {
        let mut guard = self.in_flight.lock().await;
        guard.remove(txn_id)
    }

    fn invalid_txn_status() -> Status {
        Status::invalid_argument(Self::INVALID_TXN_ID_MESSAGE)
    }
}

#[tonic::async_trait]
impl<S> RivetKv for RivetKvService<S>
where
    S: StorageEngine + 'static,
{
    async fn begin_transaction(
        &self,
        request: tonic::Request<BeginTransactionRequest>,
    ) -> Result<Response<BeginTransactionResponse>, Status> {
        let _client_id = request.into_inner().client_id;
        let txn = self.manager().begin_transaction();
        let snapshot_ts = txn.snapshot_ts();
        let txn_id = txn.id().clone();
        self.store_txn(txn).await;

        Ok(Response::new(BeginTransactionResponse {
            txn_id: txn_id.to_string(),
            snapshot_ts,
        }))
    }

    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        let txn_id = Self::parse_txn_id(&req.txn_id).map_err(|_| Self::invalid_txn_status())?;
        let mut txn = match self.take_txn(&txn_id).await {
            Some(txn) => txn,
            None => {
                return Ok(Response::new(GetResponse {
                    outcome: Some(get_response::Outcome::ErrorMessage(
                        "transaction not found".to_string(),
                    )),
                }));
            }
        };

        let result = self.manager().read(&mut txn, &req.key).await;
        self.store_txn(txn).await;

        match result {
            Ok(Some(version)) => Ok(Response::new(GetResponse {
                outcome: Some(get_response::Outcome::Success(GetSuccess {
                    found: true,
                    value: version.value,
                    commit_ts: version.commit_ts,
                })),
            })),
            Ok(None) => Ok(Response::new(GetResponse {
                outcome: Some(get_response::Outcome::Success(GetSuccess {
                    found: false,
                    value: Vec::new(),
                    commit_ts: 0,
                })),
            })),
            Err(err) => Ok(Response::new(GetResponse {
                outcome: Some(get_response::Outcome::ErrorMessage(storage_error_message(
                    err,
                ))),
            })),
        }
    }

    async fn put(
        &self,
        request: tonic::Request<PutRequest>,
    ) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        let txn_id = Self::parse_txn_id(&req.txn_id).map_err(|_| Self::invalid_txn_status())?;
        let txn = match self.take_txn(&txn_id).await {
            Some(txn) => txn,
            None => {
                return Ok(Response::new(PutResponse {
                    outcome: Some(put_response::Outcome::ErrorMessage(
                        "transaction not found".to_string(),
                    )),
                }));
            }
        };

        match self
            .manager()
            .write(&txn, req.key.clone(), req.value.clone())
            .await
        {
            Ok(()) => {
                self.store_txn(txn).await;
                Ok(Response::new(PutResponse {
                    outcome: Some(put_response::Outcome::Success(PutSuccess {})),
                }))
            }
            Err(err) => {
                self.store_txn(txn).await;
                Ok(Response::new(PutResponse {
                    outcome: Some(put_response::Outcome::ErrorMessage(storage_error_message(
                        err,
                    ))),
                }))
            }
        }
    }

    async fn commit(
        &self,
        request: tonic::Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let req = request.into_inner();
        let txn_id = Self::parse_txn_id(&req.txn_id).map_err(|_| Self::invalid_txn_status())?;
        let txn = match self.take_txn(&txn_id).await {
            Some(txn) => txn,
            None => {
                return Ok(Response::new(CommitResponse {
                    outcome: Some(commit_response::Outcome::ErrorMessage(
                        "transaction not found".to_string(),
                    )),
                }));
            }
        };

        match self.manager().commit(txn.clone()).await {
            Ok(CommitReceipt { commit_ts }) => Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::Success(CommitSuccess {
                    commit_ts,
                })),
            })),
            Err(err) => {
                self.store_txn(txn).await;
                Ok(Response::new(CommitResponse {
                    outcome: Some(commit_response::Outcome::ErrorMessage(
                        storage_error_message(err),
                    )),
                }))
            }
        }
    }

    async fn abort(
        &self,
        request: tonic::Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        let req = request.into_inner();
        let txn_id = Self::parse_txn_id(&req.txn_id).map_err(|_| Self::invalid_txn_status())?;

        match self.take_txn(&txn_id).await {
            Some(txn) => {
                self.manager().abort(txn).await;
                Ok(Response::new(AbortResponse {
                    outcome: Some(abort_response::Outcome::Success(AbortSuccess {})),
                }))
            }
            None => Ok(Response::new(AbortResponse {
                outcome: Some(abort_response::Outcome::ErrorMessage(
                    "transaction not found".to_string(),
                )),
            })),
        }
    }
}

fn storage_error_message(err: StorageError) -> String {
    match err {
        StorageError::ValidationConflict => "validation conflict".to_string(),
        StorageError::NotFound(key) => format!("key {key} not found"),
        StorageError::CorruptedState(details) => details,
        StorageError::Unimplemented => "operation not implemented".to_string(),
    }
}
