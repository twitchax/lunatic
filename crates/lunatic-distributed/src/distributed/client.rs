use std::{
    collections::HashSet,
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
    watch, Notify,
};

use crate::{
    congestion::{self, node_connection_manager, MessageChunk, NodeConnectionManager},
    control,
    distributed::message::{Request, ResponseContent, Spawn},
    quic,
};

use lunatic_process::{Process, Signal};

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

// Sending part of the message queue
pub struct BufTx {
    sender: Sender<MessageCtx>,
    // Set once the process finished. The queue is removed once its messages were sent.
    pub(crate) finished: bool,
}

/// A process monitoring a process on another node, as tracked on either node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RemoteMonitor {
    pub env: EnvironmentId,
    // The other node.
    pub node: NodeId,
    pub monitoring: ProcessId,
    pub process: ProcessId,
}

// A local process, and the processes on other nodes it monitors.
struct RemoteMonitors {
    monitoring_process: Arc<dyn Process>,
    monitored: HashSet<(NodeId, ProcessId)>,
}

// IDs of the stand-ins for monitoring processes on other nodes, which can't clash with the IDs of
// local processes.
const FIRST_MONITOR_PROXY_ID: u64 = 1 << 63;

// The response to a message, while it's awaited.
pub struct IncomingResponse {
    content: AsyncCell<ResponseContent>,
    sent_at: Instant,
    // The node the message was sent to.
    node: NodeId,
}

impl IncomingResponse {
    fn new(node: NodeId) -> Self {
        Self {
            content: AsyncCell::new(),
            sent_at: Instant::now(),
            node,
        }
    }
}

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
    pub buf_tx: DashMap<(EnvironmentId, ProcessId), BufTx>,
    // Holds the message while its being chunked
    pub in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    pub nodes_queues: DashMap<NodeId, Sender<MessageChunk>>,
    pub responses: DashMap<MessageId, Arc<IncomingResponse>>,
    pub response_tx: Sender<(MessageId, ResponseContent)>,
    pub has_messages: Arc<Notify>,
    // Changes whenever a node leaves the cluster.
    membership: watch::Sender<()>,
    // The local processes monitoring processes on other nodes.
    remote_monitors: DashMap<(EnvironmentId, ProcessId), RemoteMonitors>,
    // The local processes monitored by processes on other nodes, keyed by the node of the
    // monitoring process. Holds the ID of the stand-in for the monitoring process, and the
    // monitored process.
    monitor_proxies: DashMap<RemoteMonitor, (u64, Arc<dyn Process>)>,
    next_monitor_proxy_id: AtomicU64,
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
                membership: watch::Sender::new(()),
                remote_monitors: DashMap::new(),
                monitor_proxies: DashMap::new(),
                next_monitor_proxy_id: AtomicU64::new(FIRST_MONITOR_PROXY_ID),
            }),
        };
        let inner = Arc::downgrade(&client.inner);
        client.inner.control_client.on_node_down(move |node_id| {
            if let Some(inner) = inner.upgrade() {
                inner.node_down(NodeId(node_id));
            }
        });
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
        self.connect(node).await?;
        // Lazy initialize process message buffers
        let tx = message_queue(&self.inner.buf_tx, &self.inner.buf_rx, env, src);
        let message_id = self.next_message_id();
        // Register the response before queueing the message, so that it can't arrive first.
        if track_response {
            let response = Arc::new(IncomingResponse::new(node));
            self.inner.responses.insert(message_id, response.clone());
            // A node that's down won't respond. If it went down after this check, the response
            // was already registered to fail.
            if self.node_is_gone(node) {
                response.content.set(node_is_down(node));
            }
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

    // Queues a message without waiting, for callers that can't wait. A message to a node without
    // a connection waits for one.
    pub(crate) fn queue_message(
        &self,
        env: EnvironmentId,
        src: ProcessId,
        node: NodeId,
        dest: ProcessId,
        data: Bytes,
    ) {
        let tx = message_queue(&self.inner.buf_tx, &self.inner.buf_rx, env, src);
        let message = MessageCtx {
            message_id: self.next_message_id(),
            env,
            src,
            node,
            dest,
            offset: AtomicUsize::new(0),
            chunk_id: AtomicU64::new(0),
            data,
        };
        if tx.try_send(message).is_err() {
            log::error!("lunatic::distributed::client::queue_message");
        }
        self.inner.has_messages.notify_one();
        if !self.inner.nodes_queues.contains_key(&node) {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let client = self.clone();
                runtime.spawn(async move { client.connect(node).await });
            }
        }
    }

    // Makes sure that a connection manager forwards the messages for `node`, unless the node is
    // down. Then the congestion control worker drops its messages.
    async fn connect(&self, node: NodeId) -> Result<()> {
        if self.inner.nodes_queues.contains_key(&node) {
            return Ok(());
        }
        // Refresh nodes to be sure that target node is up to date
        self.inner.control_client.refresh_nodes().await.ok();
        let node_info = self
            .inner
            .control_client
            .node_info(node.0)
            .ok_or_else(|| anyhow!("Node does not exist"))?;
        if self.node_is_gone(node) {
            return Ok(());
        }
        self.inner.nodes_queues.entry(node).or_insert_with(|| {
            let (send, recv) = tokio::sync::mpsc::channel(1_000_000);
            tokio::spawn(node_connection_manager(NodeConnectionManager {
                streams: 10,
                node_info,
                client: self.inner.node_client.clone(),
                message_chunks: recv,
            }));
            send
        });
        Ok(())
    }

    // Whether the control server stopped listing the node, e.g. because the node stopped.
    pub(crate) fn node_is_gone(&self, node: NodeId) -> bool {
        !self.inner.control_client.node_ids().contains(&node.0)
    }

    /// Returns once `node` is down: it left the cluster, gracefully or because its lease on the
    /// control server ran out.
    pub async fn wait_node_down(&self, node: NodeId) {
        let mut membership = self.inner.membership.subscribe();
        while !self.node_is_gone(node) {
            if membership.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    /// Stops the remote monitors of a finished process, and removes its message queue once its
    /// queued messages were sent.
    pub fn remove_process_resources(&self, env: EnvironmentId, process_id: ProcessId) {
        if let Some((_, monitors)) = self.inner.remote_monitors.remove(&(env, process_id)) {
            for (node, process) in monitors.monitored {
                self.send_stop_monitoring(RemoteMonitor {
                    env,
                    node,
                    monitoring: process_id,
                    process,
                });
            }
        }
        finish_queue(&self.inner.buf_tx, env, process_id);
        self.inner.has_messages.notify_one();
    }

    /// `monitoring_process` monitors `monitor.process` on `monitor.node`. It's sent a
    /// `Signal::ProcessDied` once the monitored process dies for any reason, after the messages
    /// the monitored process sent it. Also if the process doesn't exist, or its node goes down.
    pub fn monitor(&self, monitor: RemoteMonitor, monitoring_process: Arc<dyn Process>) {
        self.inner
            .remote_monitors
            .entry((monitor.env, monitor.monitoring))
            .or_insert_with(|| RemoteMonitors {
                monitoring_process,
                monitored: HashSet::new(),
            })
            .monitored
            .insert((monitor.node, monitor.process));
        // Checked after registering, so that the node going down meanwhile isn't missed.
        if !self.node_is_gone(monitor.node) {
            self.send_monitor(monitor);
            return;
        }
        // The node may have joined since the node list was refreshed.
        let client = self.clone();
        tokio::spawn(async move {
            client.inner.control_client.refresh_nodes().await.ok();
            if client.node_is_gone(monitor.node) {
                client.remote_process_died(monitor);
            } else {
                client.send_monitor(monitor);
            }
        });
    }

    pub fn stop_monitoring(&self, monitor: RemoteMonitor) {
        let stopped = self
            .inner
            .remote_monitors
            .get_mut(&(monitor.env, monitor.monitoring))
            .is_some_and(|mut monitors| {
                monitors.monitored.remove(&(monitor.node, monitor.process))
            });
        if stopped {
            self.send_stop_monitoring(monitor);
        }
    }

    fn send_monitor(&self, monitor: RemoteMonitor) {
        let request = Request::Monitor {
            node_id: self.node_id.0,
            environment_id: monitor.env.0,
            monitoring_process_id: monitor.monitoring.0,
            process_id: monitor.process.0,
        };
        self.queue_request(monitor, request);
    }

    fn send_stop_monitoring(&self, monitor: RemoteMonitor) {
        let request = Request::StopMonitoring {
            node_id: self.node_id.0,
            environment_id: monitor.env.0,
            monitoring_process_id: monitor.monitoring.0,
            process_id: monitor.process.0,
        };
        self.queue_request(monitor, request);
    }

    // Queues a monitoring request from the monitoring process to the monitored one.
    fn queue_request(&self, monitor: RemoteMonitor, request: Request) {
        let data = match rmp_serde::to_vec(&request) {
            Ok(data) => data,
            Err(_) => unreachable!("lunatic::distributed::client::monitor serialize_message"),
        };
        self.queue_message(
            monitor.env,
            monitor.monitoring,
            monitor.node,
            monitor.process,
            data.into(),
        );
    }

    /// The monitored process of a `monitor` died, or its node went down.
    pub fn remote_process_died(&self, monitor: RemoteMonitor) {
        let monitoring_process = self
            .inner
            .remote_monitors
            .get_mut(&(monitor.env, monitor.monitoring))
            .and_then(|mut monitors| {
                monitors
                    .monitored
                    .remove(&(monitor.node, monitor.process))
                    .then(|| monitors.monitoring_process.clone())
            });
        if let Some(monitoring_process) = monitoring_process {
            monitoring_process.send(Signal::ProcessDied(monitor.process.0));
        }
    }

    /// Registers a stand-in for a process on `monitor.node` monitoring the local process
    /// `monitored_process`, to be sent to the monitored process with `Signal::Monitor`.
    pub fn monitor_proxy(
        &self,
        monitor: RemoteMonitor,
        monitored_process: Arc<dyn Process>,
    ) -> Arc<dyn Process> {
        // Monitoring a process again keeps the stand-in.
        let id = match self.inner.monitor_proxies.entry(monitor) {
            Entry::Occupied(proxy) => proxy.get().0,
            Entry::Vacant(entry) => {
                let id = self
                    .inner
                    .next_monitor_proxy_id
                    .fetch_add(1, atomic::Ordering::Relaxed);
                entry.insert((id, monitored_process));
                id
            }
        };
        Arc::new(MonitorProxy {
            id,
            monitor,
            client: self.clone(),
        })
    }

    /// Stops a process on `monitor.node` from monitoring a local process.
    pub fn remove_monitor_proxy(&self, monitor: RemoteMonitor) {
        if let Some((_, (id, monitored_process))) = self.inner.monitor_proxies.remove(&monitor) {
            monitored_process.send(Signal::StopMonitoring { process_id: id });
        }
    }

    /// The process monitored through a stand-in died, unless the stand-in already fired.
    pub fn monitored_process_died(&self, monitor: RemoteMonitor) {
        if self.inner.monitor_proxies.remove(&monitor).is_some() {
            self.notify_process_died(monitor);
        }
    }

    /// Notifies the monitoring process on `monitor.node` that the local process
    /// `monitor.process` died, after the messages the process sent it: the notice goes through
    /// the process' own message queue.
    pub fn notify_process_died(&self, monitor: RemoteMonitor) {
        let request = Request::ProcessDied {
            node_id: self.node_id.0,
            environment_id: monitor.env.0,
            monitoring_process_id: monitor.monitoring.0,
            process_id: monitor.process.0,
        };
        let data = match rmp_serde::to_vec(&request) {
            Ok(data) => data,
            Err(_) => unreachable!("lunatic::distributed::client::monitor serialize_message"),
        };
        self.queue_message(
            monitor.env,
            monitor.process,
            monitor.node,
            monitor.monitoring,
            data.into(),
        );
        // The process is dead, so the queue can go once the notice was sent.
        finish_queue(&self.inner.buf_tx, monitor.env, monitor.process);
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
    let content = response.content.take().await;
    responses.remove(&message_id);
    Ok(content)
}

// Returns the sending end of a process' message queue, creating the queue on the first message.
//
// The sender is cloned out of the map, so that no guard of it is held while sending. The queue is
// created atomically, because many tasks send the responses of a node (environment 0, process 0)
// concurrently: a second queue would replace the first one, together with its messages.
//
// A finished process' queue is reused until it's removed, so that messages sent on behalf of the
// process after it finished (e.g. notices of its death) come after the ones it sent itself.
pub(crate) fn message_queue(
    buf_tx: &DashMap<(EnvironmentId, ProcessId), BufTx>,
    buf_rx: &DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    env: EnvironmentId,
    src: ProcessId,
) -> Sender<MessageCtx> {
    match buf_tx.entry((env, src)) {
        Entry::Occupied(tx) => tx.get().sender.clone(),
        Entry::Vacant(entry) => {
            let (send, recv) = tokio::sync::mpsc::channel(1_000_000);
            buf_rx.entry(env).or_default().insert(src, recv);
            entry.insert(BufTx {
                sender: send.clone(),
                finished: false,
            });
            send
        }
    }
}

// Marks the queue of a finished process for removal.
pub(crate) fn finish_queue(
    buf_tx: &DashMap<(EnvironmentId, ProcessId), BufTx>,
    env: EnvironmentId,
    src: ProcessId,
) {
    if let Some(mut tx) = buf_tx.get_mut(&(env, src)) {
        tx.finished = true;
    }
}

// Removes the queue of a finished process if no message is left in it. Holding the sender's entry
// while removing the receiver keeps `message_queue` from reusing the queue meanwhile.
pub(crate) fn remove_queue_if_done(
    buf_tx: &DashMap<(EnvironmentId, ProcessId), BufTx>,
    buf_rx: &DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    env: EnvironmentId,
    src: ProcessId,
) {
    if let Entry::Occupied(tx) = buf_tx.entry((env, src)) {
        let empty = buf_rx
            .get(&env)
            .and_then(|env| env.get(&src).map(|recv| recv.is_empty()))
            .unwrap_or(true);
        if tx.get().finished && empty {
            if let Some(env) = buf_rx.get(&env) {
                env.remove(&src);
            }
            tx.remove();
        }
    }
}

// Stands in for a process on another node that monitors a local process.
struct MonitorProxy {
    id: u64,
    monitor: RemoteMonitor,
    client: Client,
}

impl Process for MonitorProxy {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        // Monitored processes only send their monitors `Signal::ProcessDied`.
        if let Signal::ProcessDied(_) = signal {
            self.client.monitored_process_died(self.monitor);
        }
    }
}

impl Inner {
    // Handles a node leaving the cluster.
    fn node_down(&self, node: NodeId) {
        // Stop the node's connection manager, which drops the chunks queued for it. The
        // congestion control worker drops the node's messages in progress.
        self.nodes_queues.remove(&node);
        self.has_messages.notify_one();
        // Fail the requests waiting for the node to respond.
        for response in self.responses.iter() {
            if response.node == node && !response.content.is_set() {
                response.content.set(node_is_down(node));
            }
        }
        // The processes on the node are gone for their monitors.
        let mut died = Vec::new();
        for mut monitors in self.remote_monitors.iter_mut() {
            let monitoring_process = monitors.monitoring_process.clone();
            monitors.monitored.retain(|&(monitored_node, process)| {
                if monitored_node == node {
                    died.push((monitoring_process.clone(), process));
                }
                monitored_node != node
            });
        }
        for (monitoring_process, process) in died {
            monitoring_process.send(Signal::ProcessDied(process.0));
        }
        // The node's processes don't monitor local ones anymore.
        let mut proxies = Vec::new();
        self.monitor_proxies.retain(|monitor, proxy| {
            let down = monitor.node == node;
            if down {
                proxies.push(proxy.clone());
            }
            !down
        });
        for (id, monitored_process) in proxies {
            monitored_process.send(Signal::StopMonitoring { process_id: id });
        }
        self.membership.send_replace(());
    }
}

fn node_is_down(node: NodeId) -> ResponseContent {
    ResponseContent::Error(crate::distributed::message::ClientError::Connection(
        format!("Node {} is down", node.0),
    ))
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
                cell.content.set(response);
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
        let expired = response.sent_at.elapsed() > timeout;
        // Clean up timeouts
        if expired && response.content.is_set() {
            return false;
        }
        // Set timeout response
        if expired {
            response.content.set(ResponseContent::Error(
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
            .content
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
        let response = IncomingResponse {
            sent_at: Instant::now() - age,
            ..IncomingResponse::new(NodeId(1))
        };
        if set {
            response.content.set(ResponseContent::Sent);
        }
        Arc::new(response)
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
        let timed_out = responses.get(&MessageId(1)).unwrap().content.try_take();
        assert!(matches!(
            timed_out,
            Some(ResponseContent::Error(
                crate::distributed::message::ClientError::Unexpected(_)
            ))
        ));
        assert!(responses
            .get(&MessageId(3))
            .unwrap()
            .content
            .try_take()
            .is_none());
    }
}
