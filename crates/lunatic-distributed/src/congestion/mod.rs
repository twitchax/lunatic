/// Congestion control module for distributed message chunking between competing processes.
///
/// When a process send out a message to another process on a different node
/// the message is forwarded to the Congestion control worker via a queue.
///
/// Congestion control worker picks up each message sent and routes chunks
/// to  node connection managers. For each node there exists only one connection
/// manager.
///
/// The node connection manager is responsible for routing message chunks based on
/// the source process_id and destination process_id to appropriate quic stream.
/// This ensures that all process to process messages come in order in which they
/// are being sent.
///
/// Stream task manages quic stream and writes multiple message chunks.
///
/// Topology illustration:
///
///  -----       -----
/// |  P  | ... |  P  | - Processes
///  -----       -----
///    |      /
///    |    /
///  -----
/// |  C  | - Congestion control worker
///  -----
///    | \
///    |   \
///    |     \
///    |       \
///  -----       -----
/// |  N  | ... |  N  | - Node connection managers
///  -----       -----
///    |         | \
///    |         ...
///    |
///    |
///  -----       -----
/// |  S  | ... |  S  | - Stream tasks
///  -----       -----
///
use std::{
    collections::VecDeque,
    sync::{atomic, Arc},
    time::Duration,
};

use anyhow::Result;
use dashmap::DashMap;
use lunatic_control::NodeInfo;
use tokio::sync::{
    mpsc::{self, error::TryRecvError, Receiver, Sender},
    Notify, RwLock,
};

use crate::{
    distributed::{
        self,
        client::{BufRx, EnvironmentId, MessageCtx, NodeId, ProcessId},
    },
    quic,
};

pub struct MessageChunk {
    src: ProcessId,
    dest: ProcessId,
    message_id: u64,
    message_size: u32,
    chunk_id: u64,
    data: bytes::Bytes,
}

// TODO: move to configuration
const CHUNK_SIZE: usize = 1024;

pub async fn congestion_control_worker(state: distributed::Client) -> ! {
    let inner = &state.inner;
    forward_messages(
        &inner.buf_rx,
        &inner.in_progress,
        &inner.nodes_queues,
        &inner.has_messages,
        |node| state.node_is_gone(node),
    )
    .await
}

// How long the worker waits before retrying messages it can't hand to their nodes' connections.
const MIN_BACKOFF: Duration = Duration::from_millis(1);
const MAX_BACKOFF: Duration = Duration::from_secs(1);

// Passes over the message queues of all processes, sending the next chunk of each process'
// current message to its node's connection manager, until no message is left in progress.
//
// A message whose node's connection can't take its next chunk stays in progress, so that the
// process' later messages keep their order, until its node is gone. Then it's dropped, and the
// node's connection is freed.
//
// No guard of the maps is held across an `.await`, because processes keep queueing messages (and
// creating queues) while this waits.
async fn forward_messages(
    buf_rx: &DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    in_progress: &DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    nodes_queues: &DashMap<NodeId, Sender<MessageChunk>>,
    has_messages: &Notify,
    node_is_gone: impl Fn(NodeId) -> bool,
) -> ! {
    has_messages.notified().await;
    log::trace!("starting congestion control worker");
    let mut backoff = MIN_BACKOFF;
    loop {
        let queues: Vec<(EnvironmentId, ProcessId)> = buf_rx
            .iter()
            .flat_map(|env| {
                env.iter()
                    .map(|pid| (*env.key(), *pid.key()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut progressed = false;
        let mut disconnected = vec![];
        let mut gone_nodes = vec![];
        for key in queues {
            let finished = if let Some(msg_ctx) = in_progress.get(&key) {
                // Chunk data using offset
                let offset = msg_ctx.offset.load(atomic::Ordering::Relaxed);
                let chunk_id = msg_ctx.chunk_id.load(atomic::Ordering::Relaxed);
                let (data, finished) = if msg_ctx.data.len() <= offset + CHUNK_SIZE {
                    // Chunk will be finished after this write
                    (msg_ctx.data.slice(offset..), true)
                } else {
                    (msg_ctx.data.slice(offset..offset + CHUNK_SIZE), false)
                };
                // Create chunk
                let chunk = MessageChunk {
                    src: msg_ctx.src,
                    dest: msg_ctx.dest,
                    message_id: msg_ctx.message_id.0,
                    message_size: msg_ctx.data.len() as u32,
                    chunk_id,
                    data,
                };
                let sent = nodes_queues
                    .get(&msg_ctx.node)
                    .is_some_and(|node_queue| node_queue.try_send(chunk).is_ok());
                if sent {
                    log::trace!(
                        "congestion::chunk::sent message_id={} chunk_id={chunk_id}",
                        msg_ctx.message_id.0
                    );
                    // Move to next chunk
                    msg_ctx
                        .offset
                        .store(offset + CHUNK_SIZE, atomic::Ordering::Relaxed);
                    msg_ctx
                        .chunk_id
                        .store(chunk_id + 1, atomic::Ordering::Relaxed);
                    progressed = true;
                    finished
                } else if node_is_gone(msg_ctx.node) {
                    log::warn!(
                        "Dropping message from pid={} to node={} dest_pid={}: the node is gone",
                        msg_ctx.src.0,
                        msg_ctx.node.0,
                        msg_ctx.dest.0,
                    );
                    gone_nodes.push(msg_ctx.node);
                    progressed = true;
                    true
                } else {
                    // The node's connection is behind or not there yet, retry on a later pass.
                    false
                }
            } else {
                true
            };
            if finished {
                in_progress.remove(&key);
                let next_message = buf_rx
                    .get(&key.0)
                    .and_then(|env| env.get_mut(&key.1).map(|mut recv| recv.try_recv()));
                match next_message {
                    // Push message into in progress space
                    Some(Ok(new_msg_ctx)) => {
                        log::trace!(
                            "congestion::message::received message_id={}",
                            new_msg_ctx.message_id.0
                        );
                        in_progress.insert((new_msg_ctx.env, new_msg_ctx.src), new_msg_ctx);
                        progressed = true;
                    }
                    // Process finished clean up
                    Some(Err(TryRecvError::Disconnected)) => disconnected.push(key),
                    // No new messages
                    Some(Err(TryRecvError::Empty)) | None => (),
                };
            }
        }
        // remove disconnected processes
        for (env, pid) in disconnected {
            if let Some(env) = buf_rx.get(&env) {
                env.remove(&pid);
            }
        }
        // Stops the connection managers of gone nodes.
        for node in gone_nodes {
            nodes_queues.remove(&node);
        }
        // Only wait to be woken up by the next message after a full pass found nothing to send.
        // Waiting in the middle of a pass would miss messages queued for the processes that were
        // already passed.
        if in_progress.is_empty() {
            backoff = MIN_BACKOFF;
            has_messages.notified().await;
        } else if progressed {
            backoff = MIN_BACKOFF;
            tokio::task::yield_now().await;
        } else {
            // Every message in progress waits for its node's connection. Back off instead of
            // passing over them again and again.
            tokio::time::timeout(backoff, has_messages.notified())
                .await
                .ok();
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

type StreamBuffer = Arc<RwLock<VecDeque<MessageChunk>>>;

enum StreamAction {
    Message,
    Die,
}

pub struct NodeConnectionManager {
    pub streams: usize,
    pub node_info: NodeInfo,
    pub client: quic::Client,
    pub message_chunks: Receiver<MessageChunk>,
}

// How long to wait for the peer to let a stream be opened.
const STREAM_OPEN_TIMEOUT: Duration = Duration::from_secs(1);
// How long to wait before reconnecting when no stream could be opened.
const RECONNECT_DELAY: Duration = Duration::from_secs(2);

pub async fn node_connection_manager(mut manager: NodeConnectionManager) -> Result<()> {
    let node_info = manager.node_info;
    log::trace!(
        "congestion::node_connection_manager::started node={} address={}",
        node_info.id,
        node_info.address
    );
    // The chunks that the streams of the previous connection didn't send.
    let mut unsent: Vec<StreamBuffer> = Vec::new();
    // Setup stream dead waker
    let (dead_stream_notifier, mut dead_stream_waker) = mpsc::channel::<()>(1);

    loop {
        // The node is gone when the congestion control worker freed its queue.
        if manager.message_chunks.is_closed() {
            stop_connection_manager(&node_info, &mut manager.message_chunks);
            return Ok(());
        }
        // Setup conn or fail
        let conn = match manager
            .client
            .try_connect(node_info.address, &node_info.name, 3)
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("congestion::node_connection_manager Connection failed: {e}");
                continue;
            }
        };
        log::trace!(
            "node={} name={} address={}",
            node_info.id,
            node_info.name,
            node_info.address
        );
        // Streams of the previous connection may have reported it dead after the first one did.
        while dead_stream_waker.try_recv().is_ok() {}
        // Open as many streams as the peer lets us, up to `manager.streams`.
        let mut quic_streams = Vec::with_capacity(manager.streams);
        while quic_streams.len() < manager.streams {
            match tokio::time::timeout(STREAM_OPEN_TIMEOUT, conn.open_uni()).await {
                Ok(Ok(stream)) => quic_streams.push(stream),
                Ok(Err(e)) => {
                    log::error!("congestion::node_connection_manager Stream open failed: {e}");
                    break;
                }
                Err(_) => {
                    log::error!("congestion::node_connection_manager Stream open timed out");
                    break;
                }
            }
        }
        if quic_streams.is_empty() {
            // The connection is unusable without streams.
            tokio::time::sleep(RECONNECT_DELAY).await;
            continue;
        }
        // Setup stream buffers, starting with the chunks the previous connection didn't send.
        let buffers: Vec<StreamBuffer> = quic_streams
            .iter()
            .map(|_| Arc::new(RwLock::new(VecDeque::new())))
            .collect();
        for previous in unsent.drain(..) {
            let mut previous = previous.write().await;
            // Oldest chunks first, so that the chunks of each process pair stay in order.
            while let Some(chunk) = previous.pop_back() {
                let stream_index = stream_index(&chunk, buffers.len());
                buffers[stream_index].write().await.push_front(chunk);
            }
        }
        // Start stream tasks
        let mut stream_tasks = Vec::new();
        let mut stream_wakers = Vec::new();
        for (quic_stream, buffer) in quic_streams.into_iter().zip(&buffers) {
            let (send, recv) = mpsc::channel::<StreamAction>(1000);
            if !buffer.read().await.is_empty() {
                send.try_send(StreamAction::Message).ok();
            }
            stream_wakers.push(send);
            stream_tasks.push(tokio::spawn(stream_task(StreamTask {
                quic_stream,
                action: recv,
                manager_notifier: dead_stream_notifier.clone(),
                buffer: buffer.clone(),
            })));
        }
        // Working chunk passing loop
        let mut node_is_gone = false;
        'forward_chunks: loop {
            tokio::select! {
                chunk = manager.message_chunks.recv() => {
                    let Some(chunk) = chunk else {
                        node_is_gone = true;
                        break 'forward_chunks;
                    };
                    log::trace!("congestion::node_connection_manager::msg_id {}", chunk.message_id);
                    let stream_index = stream_index(&chunk, buffers.len());
                    // Push data into message buffer
                    buffers[stream_index].write().await.push_front(chunk);
                    // Wake up stream task
                    stream_wakers[stream_index].try_send(StreamAction::Message).ok();
                },
                _ = dead_stream_waker.recv() => {
                    break 'forward_chunks;
                },
            };
        }
        // Try to wake up all remaining streams
        for stream in stream_wakers {
            stream.try_send(StreamAction::Die).ok();
        }
        // Clean up tasks
        for task in stream_tasks {
            task.await.ok();
        }
        unsent = buffers;
        if node_is_gone {
            stop_connection_manager(&node_info, &mut manager.message_chunks);
            return Ok(());
        }
    }
}

// The stream of a chunk, by its source and destination process, so that all messages between two
// processes arrive in order.
fn stream_index(chunk: &MessageChunk, streams: usize) -> usize {
    ((chunk.src.0 ^ chunk.dest.0) % streams as u64) as usize
}

// Drops the chunks still queued for a gone node.
fn stop_connection_manager(node_info: &NodeInfo, message_chunks: &mut Receiver<MessageChunk>) {
    let mut dropped = 0;
    while message_chunks.try_recv().is_ok() {
        dropped += 1;
    }
    if dropped > 0 {
        log::warn!(
            "Dropping {dropped} message chunks to node={}: the node is gone",
            node_info.id
        );
    }
    log::trace!(
        "congestion::node_connection_manager::stopped node={}",
        node_info.id
    );
}

struct StreamTask {
    quic_stream: quinn::SendStream,
    action: Receiver<StreamAction>,
    manager_notifier: Sender<()>,
    buffer: StreamBuffer,
}

async fn stream_task(mut state: StreamTask) {
    log::trace!("congestion::stream_task::start {}", state.quic_stream.id());
    while let Some(StreamAction::Message) = state.action.recv().await {
        let mut buffer = state.buffer.write().await;
        let mut chunks = Vec::new();
        while let Some(chunk) = buffer.pop_back() {
            chunks.push(chunk);
        }
        let mut data: Vec<bytes::Bytes> = chunks
            .iter()
            .flat_map(|c| {
                let mut buf = Vec::new();
                buf.extend(c.message_id.to_le_bytes().as_ref());
                buf.extend(c.message_size.to_le_bytes().as_ref());
                buf.extend(c.chunk_id.to_le_bytes().as_ref());
                buf.extend((c.data.len() as u32).to_le_bytes().as_ref());
                // buf.extend(&c.data);
                vec![bytes::Bytes::from(buf), c.data.clone()]
            })
            .collect();
        // Try to send data
        match state.quic_stream.write_all_chunks(&mut data).await {
            Ok(_) => {
                log::trace!("congestion::stream_task::write");
            }
            Err(_) => {
                // Connection is dead return chunks in order back to the buffer
                chunks.drain(..).rev().for_each(|c| buffer.push_back(c));
                // Notify manager that connection has died. One notification is enough, and the
                // manager may already be waiting for this task to finish instead of reading them.
                state.manager_notifier.try_send(()).ok();
                break;
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicU64, AtomicUsize},
            Mutex,
        },
        thread,
        time::Duration,
    };

    use super::*;
    use crate::distributed::client::MessageId;

    const NODE: NodeId = NodeId(1);
    const OTHER_NODE: NodeId = NodeId(2);

    #[derive(Default)]
    struct Queues {
        buf_rx: DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
        in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
        nodes_queues: DashMap<NodeId, Sender<MessageChunk>>,
        has_messages: Notify,
        gone_nodes: Mutex<HashSet<NodeId>>,
        // How often the worker checked whether a node is gone.
        gone_checks: AtomicUsize,
    }

    impl Queues {
        // Starts forwarding messages to a node, returning the chunks it receives.
        fn start() -> (Arc<Self>, Receiver<MessageChunk>) {
            Self::start_with_node_queue(100)
        }

        // Starts forwarding messages to a node whose queue holds up to `capacity` chunks.
        fn start_with_node_queue(capacity: usize) -> (Arc<Self>, Receiver<MessageChunk>) {
            let queues = Arc::new(Self::default());
            let (sender, chunks) = mpsc::channel(capacity);
            queues.nodes_queues.insert(NODE, sender);
            let forwarding = queues.clone();
            tokio::spawn(async move {
                forward_messages(
                    &forwarding.buf_rx,
                    &forwarding.in_progress,
                    &forwarding.nodes_queues,
                    &forwarding.has_messages,
                    |node| {
                        forwarding
                            .gone_checks
                            .fetch_add(1, atomic::Ordering::Relaxed);
                        forwarding.gone_nodes.lock().unwrap().contains(&node)
                    },
                )
                .await
            });
            (queues, chunks)
        }

        // Creates the message queue of a process.
        fn queue(&self, env: u64, pid: u64) -> Sender<MessageCtx> {
            let (sender, receiver) = mpsc::channel(100);
            self.buf_rx
                .entry(EnvironmentId(env))
                .or_default()
                .insert(ProcessId(pid), receiver);
            sender
        }

        // Queues a single-chunk message, the way `Client::new_message` does.
        async fn send(&self, queue: &Sender<MessageCtx>, env: u64, pid: u64, message_id: u64) {
            assert!(queue.send(message(env, pid, message_id)).await.is_ok());
            self.has_messages.notify_one();
        }
    }

    // A single-chunk message to `NODE`.
    fn message(env: u64, pid: u64, message_id: u64) -> MessageCtx {
        MessageCtx {
            message_id: MessageId(message_id),
            env: EnvironmentId(env),
            src: ProcessId(pid),
            node: NODE,
            dest: ProcessId(0),
            chunk_id: AtomicU64::new(0),
            offset: AtomicUsize::new(0),
            data: bytes::Bytes::from_static(b"message"),
        }
    }

    async fn next_chunk(chunks: &mut Receiver<MessageChunk>) -> MessageChunk {
        tokio::time::timeout(Duration::from_secs(5), chunks.recv())
            .await
            .expect("the message wasn't forwarded")
            .unwrap()
    }

    #[tokio::test]
    async fn messages_are_forwarded_whichever_queue_the_worker_idled_at() {
        let (queues, mut chunks) = Queues::start();
        let queue_senders = [queues.queue(0, 0), queues.queue(1, 1)];

        // After forwarding a message, the worker idles at that message's queue. One of the queues
        // is passed before the other, so a second message to it comes "behind" the worker.
        let mut message_id = 0;
        for (env, queue) in (0..).zip(&queue_senders) {
            for _ in 0..3 {
                message_id += 1;
                queues.send(queue, env, env, message_id).await;
                assert_eq!(next_chunk(&mut chunks).await.message_id, message_id);
            }
        }
    }

    #[tokio::test]
    async fn queues_can_be_created_while_the_worker_idles() {
        let (queues, mut chunks) = Queues::start();
        let queue = queues.queue(0, 0);
        queues.send(&queue, 0, 0, 1).await;
        next_chunk(&mut chunks).await;

        // Processes of new environments create their queues while the worker waits for messages.
        // Blocking on a guard the worker holds would block the creating thread for good.
        let (done, created) = std::sync::mpsc::channel();
        let creating = queues.clone();
        thread::spawn(move || {
            for env in 1..1000 {
                creating.queue(env, 0);
            }
            done.send(()).unwrap();
        });
        created
            .recv_timeout(Duration::from_secs(10))
            .expect("creating queues deadlocked");

        queues.send(&queue, 0, 0, 2).await;
        assert_eq!(next_chunk(&mut chunks).await.message_id, 2);
    }

    #[tokio::test]
    async fn messages_wait_for_a_full_node_queue() {
        let (queues, mut chunks) = Queues::start_with_node_queue(1);
        let queue_senders = [queues.queue(0, 1), queues.queue(0, 2)];
        // Both messages are forwarded before the node queue is read, which only fits one.
        for (pid, queue) in (1..).zip(&queue_senders) {
            queues.send(queue, 0, pid, pid).await;
        }
        tokio::task::yield_now().await;

        let mut message_ids = vec![
            next_chunk(&mut chunks).await.message_id,
            next_chunk(&mut chunks).await.message_id,
        ];
        message_ids.sort();
        assert_eq!(message_ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn messages_wait_for_their_nodes_connection() {
        let (queues, mut chunks) = Queues::start();
        let unconnected = queues.queue(0, 1);
        let message = MessageCtx {
            node: OTHER_NODE,
            ..message(0, 1, 1)
        };
        assert!(unconnected.send(message).await.is_ok());
        queues.has_messages.notify_one();

        // Other nodes' messages are still sent.
        let connected = queues.queue(0, 2);
        queues.send(&connected, 0, 2, 2).await;
        assert_eq!(next_chunk(&mut chunks).await.message_id, 2);

        // The worker backs off while waiting for the connection, instead of spinning.
        let checks = queues.gone_checks.load(atomic::Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let checks = queues.gone_checks.load(atomic::Ordering::Relaxed) - checks;
        assert!(checks < 50, "checked the node {checks} times");

        // The message is sent once the node is connected.
        let (sender, mut other_chunks) = mpsc::channel(100);
        queues.nodes_queues.insert(OTHER_NODE, sender);
        assert_eq!(next_chunk(&mut other_chunks).await.message_id, 1);
    }

    #[tokio::test]
    async fn messages_to_gone_nodes_are_dropped() {
        let (queues, mut chunks) = Queues::start();
        // The connection to the other node is stuck with a full queue.
        let (sender, mut other_chunks) = mpsc::channel(1);
        assert!(sender.try_send(message_chunk()).is_ok());
        queues.nodes_queues.insert(OTHER_NODE, sender);
        queues.gone_nodes.lock().unwrap().insert(OTHER_NODE);

        let to_gone_node = queues.queue(0, 1);
        let message = MessageCtx {
            node: OTHER_NODE,
            ..message(0, 1, 1)
        };
        assert!(to_gone_node.send(message).await.is_ok());
        queues.has_messages.notify_one();
        let connected = queues.queue(0, 2);
        queues.send(&connected, 0, 2, 2).await;

        assert_eq!(next_chunk(&mut chunks).await.message_id, 2);
        assert!(queues.in_progress.is_empty());
        // The gone node's connection is freed, which stops its connection manager.
        assert!(!queues.nodes_queues.contains_key(&OTHER_NODE));
        assert!(other_chunks.recv().await.is_some());
        assert!(other_chunks.recv().await.is_none());
    }

    #[tokio::test]
    async fn connection_managers_of_gone_nodes_stop() {
        let certified = rcgen::generate_simple_self_signed(vec!["node".to_string()]).unwrap();
        let cert = certified.cert.pem();
        let client =
            quic::new_quic_client(&cert, &cert, &certified.signing_key.serialize_pem()).unwrap();
        let (sender, message_chunks) = mpsc::channel(10);
        assert!(sender.try_send(message_chunk()).is_ok());
        // The congestion control worker freed the queue.
        drop(sender);

        let manager = node_connection_manager(NodeConnectionManager {
            streams: 10,
            node_info: NodeInfo {
                id: OTHER_NODE.0,
                address: "127.0.0.1:1".parse().unwrap(),
                name: "node".to_string(),
            },
            client,
            message_chunks,
        });
        tokio::time::timeout(Duration::from_secs(5), manager)
            .await
            .expect("the connection manager kept running")
            .unwrap();
    }

    fn message_chunk() -> MessageChunk {
        MessageChunk {
            src: ProcessId(0),
            dest: ProcessId(0),
            message_id: 0,
            message_size: 0,
            chunk_id: 0,
            data: bytes::Bytes::new(),
        }
    }

    #[tokio::test]
    async fn queues_of_finished_processes_are_removed_after_their_messages() {
        let (queues, mut chunks) = Queues::start();
        let queue = queues.queue(0, 1);
        // A message of three chunks, followed by another message.
        let long_message = MessageCtx {
            data: bytes::Bytes::from(vec![0; 2 * CHUNK_SIZE + 1]),
            ..message(0, 1, 1)
        };
        assert!(queue.send(long_message).await.is_ok());
        queues.send(&queue, 0, 1, 2).await;
        // Let the worker start forwarding the first message, then the process finishes.
        tokio::task::yield_now().await;
        drop(queue);

        let mut chunk_ids = Vec::new();
        for _ in 0..4 {
            let chunk = next_chunk(&mut chunks).await;
            chunk_ids.push((chunk.message_id, chunk.chunk_id));
        }
        assert_eq!(chunk_ids, vec![(1, 0), (1, 1), (1, 2), (2, 0)]);
        assert!(queues.buf_rx.get(&EnvironmentId(0)).unwrap().is_empty());
    }

    // A server whose connections let a client open up to `streams` unidirectional streams, and a
    // client for it.
    fn quic_server(streams: u32) -> (quinn::Endpoint, quic::Client) {
        let certified = rcgen::generate_simple_self_signed(vec!["node".to_string()]).unwrap();
        let (cert, key) = (certified.cert.pem(), certified.signing_key.serialize_pem());
        let mut config = quic::quic_server_config(vec![cert.clone()], &key, &cert).unwrap();
        let mut transport = quinn::TransportConfig::default();
        transport.max_concurrent_uni_streams(streams.into());
        config.transport_config(Arc::new(transport));
        let server = quinn::Endpoint::server(config, "127.0.0.1:0".parse().unwrap()).unwrap();
        let client = quic::new_quic_client(&cert, &cert, &key).unwrap();
        (server, client)
    }

    fn connection_manager(
        server: &quinn::Endpoint,
        client: quic::Client,
        message_chunks: Receiver<MessageChunk>,
    ) -> NodeConnectionManager {
        NodeConnectionManager {
            streams: 10,
            node_info: NodeInfo {
                id: OTHER_NODE.0,
                address: server.local_addr().unwrap(),
                name: "node".to_string(),
            },
            client,
            message_chunks,
        }
    }

    #[tokio::test]
    async fn chunks_are_sent_over_the_streams_that_opened() {
        // The node only lets 3 of the 10 streams be opened.
        let (server, client) = quic_server(3);
        let (sender, message_chunks) = mpsc::channel(100);
        // Messages of three chunks between ten pairs of processes, which would spread over all
        // ten streams.
        for chunk_id in 0..3u8 {
            for src in 0..10 {
                let chunk = MessageChunk {
                    src: ProcessId(src),
                    dest: ProcessId(0),
                    message_id: src,
                    message_size: 3,
                    chunk_id: chunk_id.into(),
                    data: bytes::Bytes::from(vec![chunk_id]),
                };
                assert!(sender.send(chunk).await.is_ok());
            }
        }
        tokio::spawn(node_connection_manager(connection_manager(
            &server,
            client,
            message_chunks,
        )));

        let conn = server.accept().await.unwrap().await.unwrap();
        let (received_sender, mut received) = mpsc::unbounded_channel();
        for _ in 0..3 {
            let mut recv = conn.accept_uni().await.unwrap();
            let received_sender = received_sender.clone();
            tokio::spawn(async move {
                while let Ok(chunk) = quic::read_next_stream_chunk(&mut recv).await {
                    received_sender.send((chunk.message_id, chunk.data[0])).ok();
                }
            });
        }
        let mut messages: HashMap<u64, Vec<u8>> = HashMap::new();
        for _ in 0..30 {
            let (message_id, chunk_id) =
                tokio::time::timeout(Duration::from_secs(10), received.recv())
                    .await
                    .expect("the chunks weren't sent")
                    .unwrap();
            messages.entry(message_id).or_default().push(chunk_id);
        }
        // Each message's chunks arrive in order.
        for message_id in 0..10 {
            assert_eq!(messages[&message_id], vec![0, 1, 2]);
        }
    }

    #[tokio::test]
    async fn connections_without_streams_are_retried() {
        // The node doesn't let any stream be opened.
        let (server, client) = quic_server(0);
        let (sender, message_chunks) = mpsc::channel(100);
        assert!(sender.send(message_chunk()).await.is_ok());
        let manager = tokio::spawn(node_connection_manager(connection_manager(
            &server,
            client,
            message_chunks,
        )));
        let _conn = server.accept().await.unwrap().await.unwrap();

        // The manager backs off from the unusable connection, and stops once the node is gone.
        drop(sender);
        tokio::time::timeout(Duration::from_secs(10), manager)
            .await
            .expect("the connection manager kept running")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn failing_streams_dont_block_their_manager() {
        let (server, client) = quic_server(10);
        let (conn, _server_conn) = tokio::join!(
            client._connect(server.local_addr().unwrap(), "node"),
            async { server.accept().await.unwrap().await.unwrap() }
        );
        let conn = conn.unwrap();
        let (dead_stream_notifier, mut dead_stream_waker) = mpsc::channel(1);
        let mut stream_tasks = Vec::new();
        let mut buffers = Vec::new();
        let mut stream_wakers = Vec::new();
        for _ in 0..3 {
            let buffer: StreamBuffer = Arc::new(RwLock::new(VecDeque::from([message_chunk()])));
            let (stream_waker, action) = mpsc::channel(10);
            stream_tasks.push(tokio::spawn(stream_task(StreamTask {
                quic_stream: conn.open_uni().await.unwrap(),
                action,
                manager_notifier: dead_stream_notifier.clone(),
                buffer: buffer.clone(),
            })));
            buffers.push(buffer);
            stream_wakers.push(stream_waker);
        }

        // The connection dies, so that every stream fails to send its chunk.
        conn.close(0u32.into(), b"");
        for stream_waker in &stream_wakers {
            assert!(stream_waker.send(StreamAction::Message).await.is_ok());
        }

        // Like the manager: one notification, then wait for the stream tasks to finish.
        tokio::time::timeout(Duration::from_secs(5), dead_stream_waker.recv())
            .await
            .unwrap();
        for stream_task in stream_tasks {
            tokio::time::timeout(Duration::from_secs(5), stream_task)
                .await
                .expect("a stream task blocked")
                .unwrap();
        }
        // The chunks are kept for the next connection.
        for buffer in buffers {
            assert_eq!(buffer.read().await.len(), 1);
        }
    }
}
