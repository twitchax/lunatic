use std::{
    sync::{
        atomic::{self, AtomicU64, AtomicUsize},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use async_cell::sync::AsyncCell;
use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    Notify,
};

use crate::{
    congestion::{self, node_connection_manager, MessageChunk, NodeConnectionManager},
    control,
    distributed::message::{Request, ResponseContent, Spawn},
    quic,
};

use super::message::Response;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct EnvironmentId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ProcessId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct NodeId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MessageId(pub u64);

pub struct SendParams {
    pub env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub dest: ProcessId,
    pub tag: Option<i64>,
    pub data: Vec<u8>,
}

pub struct SpawnParams {
    pub env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub spawn: Spawn,
}

pub struct ResponseParams {
    pub node_id: NodeId,
    pub response: Response,
}

pub struct MessageCtx {
    pub message_id: MessageId,
    pub env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub dest: ProcessId,
    pub chunk_id: AtomicU64,
    pub offset: AtomicUsize,
    pub data: Bytes,
}

// Receiving part of the message queue
pub(crate) type BufRx = Receiver<MessageCtx>;

type IncomingResponse = (AsyncCell<ResponseContent>, Instant);

#[derive(Clone)]
pub struct Client {
    pub node_id: NodeId,
    pub inner: Arc<Inner>,
}

pub struct Inner {
    control_client: control::Client,
    node_client: quic::Client,
    pub next_message_id: AtomicU64,
    // Across Environments and ProcessId's track message queues
    pub buf_rx: DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    // Sending part of the message queue
    pub buf_tx: DashMap<(EnvironmentId, ProcessId), Sender<MessageCtx>>,
    // Holds the message while its being chunked
    pub in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    pub nodes_queues: DashMap<NodeId, Sender<MessageChunk>>,
    pub responses: DashMap<MessageId, Arc<IncomingResponse>>,
    pub response_tx: Sender<(MessageId, ResponseContent)>,
    pub has_messages: Arc<Notify>,
}

impl Client {
    pub fn new(node_id: u64, control_client: control::Client, node_client: quic::Client) -> Self {
        let (send, recv) = tokio::sync::mpsc::channel(1000);
        let client = Self {
            node_id: NodeId(node_id),
            inner: Arc::new(Inner {
                control_client,
                node_client,
                next_message_id: AtomicU64::new(1),
                buf_rx: DashMap::new(),
                buf_tx: DashMap::new(),
                in_progress: DashMap::new(),
                nodes_queues: DashMap::new(),
                responses: DashMap::new(),
                response_tx: send,
                has_messages: Arc::new(Notify::new()),
            }),
        };
        tokio::spawn(congestion::congestion_control_worker(client.clone()));
        tokio::spawn(process_responses(client.clone(), recv));
        client
    }

    fn next_message_id(&self) -> MessageId {
        MessageId(
            self.inner
                .next_message_id
                .fetch_add(1, atomic::Ordering::Relaxed),
        )
    }

    async fn new_message(
        &self,
        env: EnvironmentId,
        src: ProcessId,
        node: NodeId,
        dest: ProcessId,
        data: Bytes,
    ) -> Result<MessageId> {
        // Lazy initialize process message buffers
        let tx = match self.inner.buf_tx.get(&(env, src)) {
            Some(tx) => tx,
            None => {
                let (send, recv) = tokio::sync::mpsc::channel(1_000_000);
                match self.inner.buf_rx.get(&env) {
                    Some(env_queue) => {
                        env_queue.insert(src, recv);
                    }
                    None => {
                        let queue = DashMap::new();
                        queue.insert(src, recv);
                        self.inner.buf_rx.insert(env, queue);
                    }
                };
                self.inner.buf_tx.insert((env, src), send);
                self.inner.buf_tx.get(&(env, src)).unwrap()
            }
        };

        let node_manager_exists = self.inner.nodes_queues.get(&node).is_none();

        if node_manager_exists {
            // Refresh nodes to be sure that target node is up to date
            self.inner.control_client.refresh_nodes().await.ok();
            let node_info = self
                .inner
                .control_client
                .node_info(node.0)
                .ok_or_else(|| anyhow!("Node does not exist"))?;
            let (send, recv) = tokio::sync::mpsc::channel(1_000_000);
            tokio::spawn(node_connection_manager(NodeConnectionManager {
                streams: 10,
                node_info,
                client: self.inner.node_client.clone(),
                message_chunks: recv,
            }));
            self.inner.nodes_queues.insert(node, send);
        }
        let message_id = self.next_message_id();
        match tx
            .send(MessageCtx {
                message_id,
                env,
                src,
                node,
                dest,
                offset: AtomicUsize::new(0),
                chunk_id: AtomicU64::new(0),
                data,
            })
            .await
        {
            Ok(_) => (),
            Err(_) => log::error!("lunatic::distributed::client::send"),
        };
        self.inner.has_messages.notify_one();
        Ok(message_id)
    }

    pub fn remove_process_resources(&self, env: EnvironmentId, process_id: ProcessId) {
        self.inner.buf_tx.remove(&(env, process_id));
    }

    // Send distributed message
    pub async fn send(&self, params: SendParams) -> Result<MessageId> {
        let message = Request::Message {
            node_id: self.node_id.0,
            environment_id: params.env.0,
            process_id: params.dest.0,
            tag: params.tag,
            data: params.data,
        };
        let data = match rmp_serde::to_vec(&message) {
            Ok(data) => data,
            Err(_) => unreachable!("lunatic::distributed::client::send serialize_message"),
        };
        self.new_message(
            params.env,
            params.src,
            params.node,
            params.dest,
            data.into(),
        )
        .await
    }

    // Send distributed spawn message
    pub async fn spawn(&self, params: SpawnParams) -> Result<MessageId> {
        let message = Request::Spawn(params.spawn);
        let data = match rmp_serde::to_vec(&message) {
            Ok(data) => data,
            Err(_) => unreachable!("lunatic::distributed::client::spawn serialize_message"),
        };
        let message_id = self
            .new_message(
                params.env,
                params.src,
                params.node,
                ProcessId(0),
                data.into(),
            )
            .await?;
        self.inner
            .responses
            .insert(message_id, Arc::new((AsyncCell::new(), Instant::now())));
        Ok(message_id)
    }

    // Send distributed response message
    pub async fn send_response(&self, params: ResponseParams) -> Result<MessageId> {
        let message = Request::Response(params.response);
        let data = match rmp_serde::to_vec(&message) {
            Ok(data) => data,
            Err(_) => unreachable!("lunatic::distributed::client::spawn serialize_message"),
        };
        self.new_message(
            EnvironmentId(0),
            ProcessId(0),
            params.node_id,
            ProcessId(0),
            data.into(),
        )
        .await
    }

    // Receive response
    pub async fn recv_response(&self, response: Response) {
        self.inner
            .response_tx
            .send((MessageId(response.message_id), response.content))
            .await
            .ok();
    }

    pub async fn await_response(&self, message_id: MessageId) -> Result<ResponseContent> {
        let response = self
            .inner
            .responses
            .get(&message_id)
            .ok_or_else(|| anyhow!("message does not exist"))?
            .0
            .take()
            .await;
        self.inner.responses.remove(&message_id);
        Ok(response)
    }
}

pub async fn process_responses(
    client: Client,
    mut recv: Receiver<(MessageId, ResponseContent)>,
) -> ! {
    loop {
        tokio::select! {
           r =  recv.recv() => {
            if let Some((message_id, response)) = r {
                if let Some(cell) = client.inner.responses.get(&message_id) {
                cell.0.set(response);
                }
            }
           },
           _ = tokio::time::sleep(RESPONSE_TIMEOUT) => {
            expire_responses(&client.inner.responses, RESPONSE_TIMEOUT);
           }
        };
    }
}

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);

// Fails the responses that didn't arrive within `timeout`, and cleans up the ones that weren't
// taken in time after that.
fn expire_responses(responses: &DashMap<MessageId, Arc<IncomingResponse>>, timeout: Duration) {
    // Removing entries while iterating over the map deadlocks on its shard locks, so retain the
    // entries to keep instead.
    responses.retain(|_, response| {
        let expired = response.1.elapsed() > timeout;
        // Clean up timeouts
        if expired && response.0.is_set() {
            return false;
        }
        // Set timeout response
        if expired {
            response.0.set(ResponseContent::Error(
                crate::distributed::message::ClientError::Unexpected(
                    "Response timeout.".to_string(),
                ),
            ));
        }
        true
    });
}

#[cfg(test)]
mod tests {
    use std::{sync::mpsc, thread};

    use super::*;

    fn response(set: bool, age: Duration) -> Arc<IncomingResponse> {
        let cell = AsyncCell::new();
        if set {
            cell.set(ResponseContent::Sent);
        }
        Arc::new((cell, Instant::now() - age))
    }

    #[test]
    fn expired_responses_time_out_and_get_cleaned_up() {
        let responses = Arc::new(DashMap::new());
        responses.insert(MessageId(1), response(false, Duration::from_secs(10)));
        responses.insert(MessageId(2), response(true, Duration::from_secs(10)));
        responses.insert(MessageId(3), response(false, Duration::ZERO));
        responses.insert(MessageId(4), response(true, Duration::ZERO));

        // A deadlock would block the expiring thread for good.
        let (done, expired) = mpsc::channel();
        let expiring = responses.clone();
        thread::spawn(move || {
            expire_responses(&expiring, RESPONSE_TIMEOUT);
            done.send(()).unwrap();
        });
        expired
            .recv_timeout(Duration::from_secs(10))
            .expect("expiring the responses deadlocked");

        let mut message_ids: Vec<_> = responses.iter().map(|entry| entry.key().0).collect();
        message_ids.sort();
        assert_eq!(message_ids, vec![1, 3, 4]);
        let timed_out = responses.get(&MessageId(1)).unwrap().0.try_take();
        assert!(matches!(
            timed_out,
            Some(ResponseContent::Error(
                crate::distributed::message::ClientError::Unexpected(_)
            ))
        ));
        assert!(responses.get(&MessageId(3)).unwrap().0.try_take().is_none());
    }
}
