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
use dashmap::{mapref::entry::Entry, DashMap};
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

    // Queues a message. If `track_response`, its response can be awaited with `await_response`.
    async fn new_message(
        &self,
        env: EnvironmentId,
        src: ProcessId,
        node: NodeId,
        dest: ProcessId,
        data: Bytes,
        track_response: bool,
    ) -> Result<MessageId> {
        // Lazy initialize process message buffers
        let tx = message_queue(&self.inner.buf_tx, &self.inner.buf_rx, env, src);

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
        // Register the response before queueing the message, so that it can't arrive first.
        if track_response {
            self.inner
                .responses
                .insert(message_id, Arc::new((AsyncCell::new(), Instant::now())));
        }
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

    // Whether the control server stopped listing the node, e.g. because the node stopped.
    pub(crate) fn node_is_gone(&self, node: NodeId) -> bool {
        !self.inner.control_client.node_ids().contains(&node.0)
    }

    pub fn remove_process_resources(&self, env: EnvironmentId, process_id: ProcessId) {
        self.inner.buf_tx.remove(&(env, process_id));
    }

    // Send distributed message
    pub async fn send(&self, params: SendParams) -> Result<MessageId> {
        self.send_message(params, false).await
    }

    // Send distributed message, and track its delivery: `await_response` returns
    // `ResponseContent::Sent` once it was delivered, or the error that prevented it.
    pub async fn send_tracked(&self, params: SendParams) -> Result<MessageId> {
        self.send_message(params, true).await
    }

    async fn send_message(&self, params: SendParams, track_response: bool) -> Result<MessageId> {
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
            track_response,
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
        self.new_message(
            params.env,
            params.src,
            params.node,
            ProcessId(0),
            data.into(),
            true,
        )
        .await
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
            false,
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
        take_response(&self.inner.responses, message_id).await
    }

    // Stops tracking the response to a message, when it's not awaited.
    pub fn forget_response(&self, message_id: MessageId) {
        self.inner.responses.remove(&message_id);
    }
}

// Waits for the response to a message and removes it. The response is cloned out of the map, so
// that no guard of it is held while waiting: other messages' responses are inserted and removed
// meanwhile.
async fn take_response(
    responses: &DashMap<MessageId, Arc<IncomingResponse>>,
    message_id: MessageId,
) -> Result<ResponseContent> {
    let response = responses
        .get(&message_id)
        .map(|response| response.clone())
        .ok_or_else(|| anyhow!("message does not exist"))?;
    let content = response.0.take().await;
    responses.remove(&message_id);
    Ok(content)
}

// Returns the sending end of a process' message queue, creating the queue on the first message.
//
// The sender is cloned out of the map, so that no guard of it is held while sending. The queue is
// created atomically, because many tasks send the responses of a node (environment 0, process 0)
// concurrently: a second queue would replace the first one, together with its messages.
fn message_queue(
    buf_tx: &DashMap<(EnvironmentId, ProcessId), Sender<MessageCtx>>,
    buf_rx: &DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    env: EnvironmentId,
    src: ProcessId,
) -> Sender<MessageCtx> {
    match buf_tx.entry((env, src)) {
        Entry::Occupied(tx) => tx.get().clone(),
        Entry::Vacant(entry) => {
            let (send, recv) = tokio::sync::mpsc::channel(1_000_000);
            buf_rx.entry(env).or_default().insert(src, recv);
            entry.insert(send.clone());
            send
        }
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
    use std::{
        sync::{mpsc, Barrier},
        thread,
    };

    use super::*;

    fn message(message_id: u64) -> MessageCtx {
        MessageCtx {
            message_id: MessageId(message_id),
            env: EnvironmentId(0),
            src: ProcessId(0),
            node: NodeId(1),
            dest: ProcessId(0),
            chunk_id: AtomicU64::new(0),
            offset: AtomicUsize::new(0),
            data: Bytes::new(),
        }
    }

    #[tokio::test]
    async fn responses_can_be_added_while_waiting_for_one() {
        let responses = Arc::new(DashMap::new());
        responses.insert(MessageId(1), response(false, Duration::ZERO));
        let waiting = responses.clone();
        let waiter = tokio::spawn(async move { take_response(&waiting, MessageId(1)).await });
        // Let the waiter start waiting.
        tokio::task::yield_now().await;

        // Spawning processes adds the responses to wait for. Blocking on a guard the waiter holds
        // would block the adding thread for good.
        let (done, added) = mpsc::channel();
        let adding = responses.clone();
        thread::spawn(move || {
            for message_id in 2..1000 {
                adding.insert(MessageId(message_id), response(false, Duration::ZERO));
            }
            done.send(()).unwrap();
        });
        added
            .recv_timeout(Duration::from_secs(10))
            .expect("adding responses deadlocked");

        responses
            .get(&MessageId(1))
            .unwrap()
            .0
            .set(ResponseContent::Sent);
        let content = waiter.await.unwrap().unwrap();
        assert!(matches!(content, ResponseContent::Sent));
        assert!(!responses.contains_key(&MessageId(1)));
    }

    #[test]
    fn concurrently_created_queues_are_the_same_queue() {
        const SENDERS: u64 = 8;
        for _ in 0..100 {
            let buf_tx = DashMap::new();
            let buf_rx = DashMap::new();
            let start = Barrier::new(SENDERS as usize);
            thread::scope(|scope| {
                for message_id in 0..SENDERS {
                    let (buf_tx, buf_rx, start) = (&buf_tx, &buf_rx, &start);
                    scope.spawn(move || {
                        start.wait();
                        let queue = message_queue(buf_tx, buf_rx, EnvironmentId(0), ProcessId(0));
                        assert!(queue.try_send(message(message_id)).is_ok());
                    });
                }
            });

            let env = buf_rx.get(&EnvironmentId(0)).unwrap();
            let mut queue = env.get_mut(&ProcessId(0)).unwrap();
            let mut received = 0;
            while queue.try_recv().is_ok() {
                received += 1;
            }
            assert_eq!(received, SENDERS);
        }
    }

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
