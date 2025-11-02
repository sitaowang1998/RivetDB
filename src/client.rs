use std::sync::Arc;

use thiserror::Error;
use tonic::transport::Channel;

use crate::rpc::service::rivet_kv_client::RivetKvClient;
use crate::rpc::service::{
    AbortRequest, AbortResponse, BeginTransactionRequest, CommitRequest, CommitResponse,
    GetRequest, GetResponse, PutRequest, abort_response, commit_response, get_response,
    put_response,
};
use crate::transaction::CommitReceipt;
use crate::types::{Timestamp, TxnId, Value};

/// Configuration for establishing a client connection to a RivetDB node.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub endpoint: String,
}

impl ClientConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

/// Errors surfaced by the asynchronous client library.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("invalid transaction id returned by server: {0}")]
    InvalidTxnId(String),
    #[error("{operation} failed: {message}")]
    OperationFailed {
        operation: &'static str,
        message: String,
    },
    #[error("malformed {0} response from server")]
    MalformedResponse(&'static str),
}

#[derive(Clone)]
pub struct RivetClient {
    inner: Arc<RivetKvClient<Channel>>,
}

impl RivetClient {
    pub async fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let client = RivetKvClient::connect(config.endpoint.clone()).await?;
        Ok(Self {
            inner: Arc::new(client),
        })
    }

    pub async fn begin_transaction(
        &self,
        client_id: impl Into<String>,
    ) -> Result<ClientTransaction, ClientError> {
        let mut client = self.inner.as_ref().clone();
        let response = client
            .begin_transaction(BeginTransactionRequest {
                client_id: client_id.into(),
            })
            .await?
            .into_inner();

        let txn_id = response
            .txn_id
            .parse::<TxnId>()
            .map_err(|_| ClientError::InvalidTxnId(response.txn_id.clone()))?;

        Ok(ClientTransaction {
            client: self.clone(),
            txn_id,
            snapshot_ts: response.snapshot_ts,
        })
    }
}

/// Result of a successful `get` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetResult {
    pub value: Value,
    pub commit_ts: Timestamp,
}

/// Client-side representation of an active transaction.
#[derive(Clone)]
pub struct ClientTransaction {
    client: RivetClient,
    txn_id: TxnId,
    snapshot_ts: Timestamp,
}

impl ClientTransaction {
    pub fn id(&self) -> &TxnId {
        &self.txn_id
    }

    pub fn snapshot_ts(&self) -> Timestamp {
        self.snapshot_ts
    }

    pub async fn get(&self, key: impl Into<String>) -> Result<Option<GetResult>, ClientError> {
        let mut client = self.client.inner.as_ref().clone();
        let response: GetResponse = client
            .get(GetRequest {
                txn_id: self.txn_id.to_string(),
                key: key.into(),
            })
            .await?
            .into_inner();

        match response.outcome {
            Some(get_response::Outcome::Success(success)) => {
                if success.found {
                    Ok(Some(GetResult {
                        value: success.value,
                        commit_ts: success.commit_ts,
                    }))
                } else {
                    Ok(None)
                }
            }
            Some(get_response::Outcome::ErrorMessage(message)) => {
                Err(ClientError::OperationFailed {
                    operation: "get",
                    message,
                })
            }
            None => Err(ClientError::MalformedResponse("get")),
        }
    }

    pub async fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<(), ClientError> {
        let mut client = self.client.inner.as_ref().clone();
        let response = client
            .put(PutRequest {
                txn_id: self.txn_id.to_string(),
                key: key.into(),
                value: value.into(),
            })
            .await?
            .into_inner();

        match response.outcome {
            Some(put_response::Outcome::Success(_)) => Ok(()),
            Some(put_response::Outcome::ErrorMessage(message)) => {
                Err(ClientError::OperationFailed {
                    operation: "put",
                    message,
                })
            }
            None => Err(ClientError::MalformedResponse("put")),
        }
    }

    pub async fn commit(self) -> Result<CommitReceipt, ClientError> {
        let mut client = self.client.inner.as_ref().clone();
        let response: CommitResponse = client
            .commit(CommitRequest {
                txn_id: self.txn_id.to_string(),
            })
            .await?
            .into_inner();

        match response.outcome {
            Some(commit_response::Outcome::Success(success)) => Ok(CommitReceipt {
                commit_ts: success.commit_ts,
            }),
            Some(commit_response::Outcome::ErrorMessage(message)) => {
                Err(ClientError::OperationFailed {
                    operation: "commit",
                    message,
                })
            }
            None => Err(ClientError::MalformedResponse("commit")),
        }
    }

    pub async fn abort(self) -> Result<(), ClientError> {
        let mut client = self.client.inner.as_ref().clone();
        let response: AbortResponse = client
            .abort(AbortRequest {
                txn_id: self.txn_id.to_string(),
            })
            .await?
            .into_inner();

        match response.outcome {
            Some(abort_response::Outcome::Success(_)) => Ok(()),
            Some(abort_response::Outcome::ErrorMessage(message)) => {
                Err(ClientError::OperationFailed {
                    operation: "abort",
                    message,
                })
            }
            None => Err(ClientError::MalformedResponse("abort")),
        }
    }
}
