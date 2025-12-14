use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tonic::{Response, Status};

use crate::node::{CommitError, NodeRole, RivetNode};
use crate::rpc::service::rivet_kv_client::RivetKvClient;
use crate::storage::{StorageEngine, StorageError};
use crate::transaction::{
    CollectedTransaction, CommitReceipt, Snapshot, TransactionContext, TransactionManager,
    TransactionMetadata, WriteIntent,
};
use crate::types::TxnId;

use super::service::rivet_kv_server::RivetKv;
use super::service::{
    AbortRequest, AbortResponse, AbortSuccess, BeginTransactionRequest, BeginTransactionResponse,
    CommitRequest, CommitResponse, CommitSuccess, GetRequest, GetResponse, GetSuccess, PutRequest,
    PutResponse, PutSuccess, ShipTransactionRequest, WriteMutation, abort_response,
    commit_response, get_response, put_response,
};

/// gRPC service that bridges the transaction manager with network clients.
pub struct RivetKvService<S: StorageEngine> {
    node: Arc<RivetNode<S>>,
    in_flight: Mutex<HashMap<TxnId, TransactionContext>>,
}

impl<S: StorageEngine + 'static> RivetKvService<S> {
    const INVALID_TXN_ID_MESSAGE: &'static str = "invalid transaction id";
    const NOT_SERVING_MESSAGE: &'static str =
        "node is catching up; please retry against another node";

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

    fn not_serving_message(&self) -> Option<String> {
        if self.node.role() == NodeRole::Learner {
            Some(Self::NOT_SERVING_MESSAGE.to_string())
        } else {
            None
        }
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

    async fn forward_commit(
        &self,
        collected: CollectedTransaction,
    ) -> Result<CommitReceipt, String> {
        let mut attempts = 0;
        loop {
            attempts += 1;

            let leader_url = match self.node.leader_endpoint_url() {
                Some(url) => url,
                None => {
                    if attempts >= 5 {
                        return Err("leader unknown".to_string());
                    }
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            let mut client = match RivetKvClient::connect(leader_url.clone())
                .await
                .map_err(|err| err.to_string())
            {
                Ok(client) => client,
                Err(message) => {
                    if attempts >= 5 || !Self::should_retry_forward(&message) {
                        return Err(message);
                    }
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            let request = Self::build_ship_request(&collected);
            let response = match client.ship_transaction(request).await {
                Ok(resp) => resp.into_inner(),
                Err(status) => {
                    let message = status.to_string();
                    if attempts >= 5 || !Self::should_retry_forward(&message) {
                        return Err(message);
                    }
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            match response.outcome {
                Some(commit_response::Outcome::Success(CommitSuccess { commit_ts })) => {
                    return Ok(CommitReceipt { commit_ts });
                }
                Some(commit_response::Outcome::ErrorMessage(message)) => {
                    if attempts >= 5 || !Self::should_retry_forward(&message) {
                        return Err(message);
                    }
                    sleep(Duration::from_millis(100)).await;
                }
                None => {
                    if attempts >= 5 {
                        return Err("malformed leader response".to_string());
                    }
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    fn build_ship_request(collected: &CollectedTransaction) -> ShipTransactionRequest {
        ShipTransactionRequest {
            txn_id: collected.metadata.id().to_string(),
            snapshot_ts: collected.metadata.snapshot_ts(),
            read_keys: collected.metadata.read_keys().to_vec(),
            writes: collected
                .writes
                .iter()
                .map(|w| WriteMutation {
                    key: w.key.clone(),
                    value: w.value.clone(),
                })
                .collect(),
        }
    }

    fn should_retry_forward(message: &str) -> bool {
        if message.contains("validation conflict") {
            return false;
        }
        if message.contains("transaction not found") {
            return false;
        }
        true
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
        if let Some(message) = self.not_serving_message() {
            return Err(Status::unavailable(message));
        }

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
        if let Some(message) = self.not_serving_message() {
            return Ok(Response::new(GetResponse {
                outcome: Some(get_response::Outcome::ErrorMessage(message)),
            }));
        }

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
        if let Some(message) = self.not_serving_message() {
            return Ok(Response::new(PutResponse {
                outcome: Some(put_response::Outcome::ErrorMessage(message)),
            }));
        }

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
        if let Some(message) = self.not_serving_message() {
            return Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::ErrorMessage(message)),
            }));
        }

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

        match self.manager().collect(txn).await {
            Ok(collected) => {
                if self.node.role() == NodeRole::Leader {
                    match self.node.replicate_commit(collected).await {
                        Ok(CommitReceipt { commit_ts }) => Ok(Response::new(CommitResponse {
                            outcome: Some(commit_response::Outcome::Success(CommitSuccess {
                                commit_ts,
                            })),
                        })),
                        Err(err) => Ok(Response::new(CommitResponse {
                            outcome: Some(commit_response::Outcome::ErrorMessage(
                                commit_error_message(err),
                            )),
                        })),
                    }
                } else {
                    match self.forward_commit(collected).await {
                        Ok(CommitReceipt { commit_ts }) => Ok(Response::new(CommitResponse {
                            outcome: Some(commit_response::Outcome::Success(CommitSuccess {
                                commit_ts,
                            })),
                        })),
                        Err(message) => Ok(Response::new(CommitResponse {
                            outcome: Some(commit_response::Outcome::ErrorMessage(message)),
                        })),
                    }
                }
            }
            Err(err) => Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::ErrorMessage(
                    storage_error_message(err),
                )),
            })),
        }
    }

    async fn abort(
        &self,
        request: tonic::Request<AbortRequest>,
    ) -> Result<Response<AbortResponse>, Status> {
        if let Some(message) = self.not_serving_message() {
            return Ok(Response::new(AbortResponse {
                outcome: Some(abort_response::Outcome::ErrorMessage(message)),
            }));
        }

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

    async fn ship_transaction(
        &self,
        request: tonic::Request<ShipTransactionRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        if let Some(message) = self.not_serving_message() {
            return Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::ErrorMessage(message)),
            }));
        }

        let req = request.into_inner();
        let txn_id = Self::parse_txn_id(&req.txn_id).map_err(|_| Self::invalid_txn_status())?;

        let mut metadata = TransactionMetadata::new(txn_id, Snapshot::new(req.snapshot_ts));
        for key in req.read_keys {
            metadata.record_read(key);
        }

        let writes = req
            .writes
            .into_iter()
            .map(|w| WriteIntent {
                key: w.key,
                value: w.value,
            })
            .collect::<Vec<_>>();

        let collected = CollectedTransaction { metadata, writes };

        match self.node.replicate_commit(collected).await {
            Ok(CommitReceipt { commit_ts }) => Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::Success(CommitSuccess {
                    commit_ts,
                })),
            })),
            Err(err) => Ok(Response::new(CommitResponse {
                outcome: Some(commit_response::Outcome::ErrorMessage(
                    commit_error_message(err),
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

fn commit_error_message(err: CommitError) -> String {
    err.to_string()
}
