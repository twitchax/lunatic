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
};

use anyhow::Result;
use dashmap::DashMap;
use lunatic_control::NodeInfo;
use tokio::sync::{
    mpsc::{
        self,
        error::{TryRecvError, TrySendError},
        Receiver, Sender,
    },
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
    )
    .await
}

// Passes over the message queues of all processes, sending the next chunk of each process'
// current message to its node's connection manager, until no message is left in progress.
//
// No guard of the maps is held across an `.await`, because processes keep queueing messages (and
// creating queues) while this waits.
async fn forward_messages(
    buf_rx: &DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
    in_progress: &DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    nodes_queues: &DashMap<NodeId, Sender<MessageChunk>>,
    has_messages: &Notify,
) -> ! {
    has_messages.notified().await;
    log::trace!("starting congestion control worker");
    loop {
        let queues: Vec<(EnvironmentId, ProcessId)> = buf_rx
            .iter()
            .flat_map(|env| {
                env.iter()
                    .map(|pid| (*env.key(), *pid.key()))
                    .collect::<Vec<_>>()
            })
            .collect();
        let mut disconnected = vec![];
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
                if let Some(node_queue) = nodes_queues.get(&msg_ctx.node) {
                    match node_queue.try_send(chunk) {
                        Ok(_) => {
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
                            finished
                        }
                        // The connection manager is behind, retry the chunk on the next pass.
                        Err(TrySendError::Full(_)) => false,
                        Err(e) => {
                            log::warn!(
                                "Cannot send next chunk from pid={} to node={} dest_pid={}, reason: {e}",
                                msg_ctx.src.0,
                                msg_ctx.node.0,
                                msg_ctx.dest.0,
                            );
                            // Without a connection manager the message can't be sent anymore.
                            true
                        }
                    }
                } else {
                    log::error!("Connection to node={} does not exist", msg_ctx.node.0);
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
        // Only wait to be woken up by the next message after a full pass found nothing to send.
        // Waiting in the middle of a pass would miss messages queued for the processes that were
        // already passed.
        if in_progress.is_empty() {
            has_messages.notified().await;
        } else {
            tokio::task::yield_now().await;
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

pub async fn node_connection_manager(mut manager: NodeConnectionManager) -> Result<()> {
    let node_info = manager.node_info;
    log::trace!(
        "congestion::node_connection_manager::started node={} address={}",
        node_info.id,
        node_info.address
    );
    // Setup stream buffer
    let mut buffers: Vec<StreamBuffer> = Vec::with_capacity(manager.streams);
    for _ in 0..manager.streams {
        buffers.push(Arc::new(RwLock::new(VecDeque::new())));
    }
    // Setup stream dead waker
    let (dead_stream_notifier, mut dead_stream_waker) = mpsc::channel::<()>(1);

    loop {
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
        // Start stream tasks
        let mut stream_tasks = Vec::new();
        let mut stream_wakers = Vec::new();
        for buffer in buffers.iter().take(manager.streams) {
            let stream = match conn.open_uni().await {
                Ok(stream) => stream,
                Err(e) => {
                    log::error!("congestion::node_connection_manager Stream open failed: {e}");
                    continue;
                }
            };
            let (send, recv) = mpsc::channel::<StreamAction>(1000);
            stream_wakers.push(send);
            stream_tasks.push(tokio::spawn(stream_task(StreamTask {
                quic_stream: stream,
                action: recv,
                manager_notifier: dead_stream_notifier.clone(),
                buffer: buffer.clone(),
            })));
        }
        // Working chunk passing loop
        'forward_chunks: loop {
            tokio::select! {
                Some(chunk) = manager.message_chunks.recv() => {
                    log::trace!("congestion::node_connection_manager::msg_id {}", chunk.message_id);
                    let src = chunk.src.0;
                    let dest = chunk.dest.0;
                    // Determine stream index by source and destination process_id
                    // This ensures that all messages arrive in order between processes
                    let stream_index = ((src ^ dest) % manager.streams as u64) as usize;
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
    }
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
                // Notify manager that connection has died
                state.manager_notifier.send(()).await.ok();
                break;
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicU64, AtomicUsize},
        thread,
        time::Duration,
    };

    use super::*;
    use crate::distributed::client::MessageId;

    const NODE: NodeId = NodeId(1);

    #[derive(Default)]
    struct Queues {
        buf_rx: DashMap<EnvironmentId, DashMap<ProcessId, BufRx>>,
        in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
        nodes_queues: DashMap<NodeId, Sender<MessageChunk>>,
        has_messages: Notify,
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
            let message = MessageCtx {
                message_id: MessageId(message_id),
                env: EnvironmentId(env),
                src: ProcessId(pid),
                node: NODE,
                dest: ProcessId(0),
                chunk_id: AtomicU64::new(0),
                offset: AtomicUsize::new(0),
                data: bytes::Bytes::from_static(b"message"),
            };
            assert!(queue.send(message).await.is_ok());
            self.has_messages.notify_one();
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
}
