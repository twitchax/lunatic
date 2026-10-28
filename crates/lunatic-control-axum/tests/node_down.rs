//! A node's requests to another node fail once the other node leaves the cluster.

use std::{collections::HashMap, net::TcpListener, sync::Arc, time::Duration};

use lunatic_control_axum::server::control_server_from_tcp;
use lunatic_distributed::{
    control,
    distributed::{
        self,
        client::{EnvironmentId, MessageId, NodeId, ProcessId, RemoteMonitor, SpawnParams},
        message::{ClientError, ResponseContent, Spawn},
        server::gen_node_cert,
    },
    quic,
};
use lunatic_process::{Process, Signal};
use tokio::sync::mpsc::UnboundedReceiver;
use uuid::Uuid;

// Registers and starts a node with the control server at `control_url`.
async fn start_node(control_url: &str) -> (control::Client, distributed::Client) {
    let http_client = reqwest::Client::new();
    let node_name = Uuid::new_v4();
    let (csr, key) = gen_node_cert(&node_name.to_string()).unwrap();
    let reg = control::Client::register(
        &http_client,
        control_url.parse().unwrap(),
        node_name,
        csr.pem().unwrap(),
    )
    .await
    .unwrap();
    let quic_client =
        quic::new_quic_client(&reg.root_cert, &reg.cert_pem_chain[0], &key.serialize_pem())
            .unwrap();
    // Nothing listens at the node's address, so its requests are never answered.
    let node_address = "127.0.0.1:1".parse().unwrap();
    let control_client = control::Client::new(http_client, reg, node_address, HashMap::new())
        .await
        .unwrap();
    let node_client = distributed::Client::new(
        control_client.node_id(),
        control_client.clone(),
        quic_client,
    );
    (control_client, node_client)
}

async fn spawn_on(node_client: &distributed::Client, node_id: u64) -> MessageId {
    node_client
        .spawn(SpawnParams {
            env: EnvironmentId(1),
            src: ProcessId(1),
            node: NodeId(node_id),
            spawn: Spawn {
                response_node_id: node_client.node_id.0,
                environment_id: 1,
                module_id: 1,
                function: "main".to_string(),
                params: Vec::new(),
                config: Vec::new(),
            },
        })
        .await
        .unwrap()
}

fn is_node_down(response: &ResponseContent) -> bool {
    matches!(
        response,
        ResponseContent::Error(ClientError::Connection(cause)) if cause.contains("is down")
    )
}

#[tokio::test]
async fn requests_fail_when_their_node_goes_down() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));
    let (leaving, _) = start_node(&control_url).await;
    let (_, node_client) = start_node(&control_url).await;

    // A spawn waiting for the node, when it leaves the cluster.
    let pending = spawn_on(&node_client, leaving.node_id()).await;
    leaving.notify_node_stopped().await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        node_client.wait_node_down(NodeId(leaving.node_id())),
    )
    .await
    .expect("the node wasn't reported down");
    let response =
        tokio::time::timeout(Duration::from_secs(1), node_client.await_response(pending))
            .await
            .expect("the request kept waiting")
            .unwrap();
    assert!(is_node_down(&response), "{response:?}");
    assert!(!node_client
        .inner
        .nodes_queues
        .contains_key(&NodeId(leaving.node_id())));

    // A spawn on the node after it left.
    let late = spawn_on(&node_client, leaving.node_id()).await;
    let response = tokio::time::timeout(Duration::from_secs(1), node_client.await_response(late))
        .await
        .expect("the request waited for a node that's down")
        .unwrap();
    assert!(is_node_down(&response), "{response:?}");
}

// A process that forwards the signals it's sent.
struct SignalRecorder {
    id: u64,
    signals: tokio::sync::mpsc::UnboundedSender<Signal>,
}

impl Process for SignalRecorder {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        self.signals.send(signal).ok();
    }
}

fn signal_recorder(id: u64) -> (Arc<dyn Process>, UnboundedReceiver<Signal>) {
    let (signals, recorded) = tokio::sync::mpsc::unbounded_channel();
    (Arc::new(SignalRecorder { id, signals }), recorded)
}

async fn process_died(signals: &mut UnboundedReceiver<Signal>) -> u64 {
    let signal = tokio::time::timeout(Duration::from_secs(5), signals.recv())
        .await
        .expect("the monitor didn't fire")
        .unwrap();
    match signal {
        Signal::ProcessDied(process_id) => process_id,
        signal => panic!("unexpected signal {signal:?}"),
    }
}

#[tokio::test]
async fn remote_monitors_fire_when_their_node_goes_down() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));
    let (leaving, _) = start_node(&control_url).await;
    let (_, node_client) = start_node(&control_url).await;
    let monitor = |monitoring: u64, process: u64| RemoteMonitor {
        env: EnvironmentId(1),
        node: NodeId(leaving.node_id()),
        monitoring: ProcessId(monitoring),
        process: ProcessId(process),
    };

    let (monitoring, mut signals) = signal_recorder(1);
    node_client.monitor(monitor(1, 7), monitoring);
    let (stopped, mut stopped_signals) = signal_recorder(2);
    node_client.monitor(monitor(2, 7), stopped);
    node_client.stop_monitoring(monitor(2, 7));
    let (finished, mut finished_signals) = signal_recorder(3);
    node_client.monitor(monitor(3, 7), finished);
    node_client.remove_process_resources(EnvironmentId(1), ProcessId(3));

    leaving.notify_node_stopped().await.unwrap();
    assert_eq!(process_died(&mut signals).await, 7);
    // A notice of the process' death that comes late doesn't fire the monitor again.
    node_client.remote_process_died(monitor(1, 7));
    assert!(signals.try_recv().is_err());
    // Monitors that were stopped, or whose process finished, don't fire.
    assert!(stopped_signals.try_recv().is_err());
    assert!(finished_signals.try_recv().is_err());

    // Monitoring a process on a node that's down fires right away.
    let (late, mut late_signals) = signal_recorder(4);
    node_client.monitor(monitor(4, 8), late);
    assert_eq!(process_died(&mut late_signals).await, 8);
}

#[tokio::test]
async fn monitored_processes_notify_their_remote_monitors_once() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));
    let (monitoring_node, _) = start_node(&control_url).await;
    let (_, node_client) = start_node(&control_url).await;
    let monitor = RemoteMonitor {
        env: EnvironmentId(1),
        node: NodeId(monitoring_node.node_id()),
        monitoring: ProcessId(1),
        process: ProcessId(7),
    };

    let (monitored, mut monitored_signals) = signal_recorder(7);
    let proxy = node_client.monitor_proxy(monitor, monitored.clone());
    // Monitoring the process again keeps the stand-in.
    assert_eq!(
        node_client.monitor_proxy(monitor, monitored).id(),
        proxy.id()
    );
    // The monitored process dies, possibly noticed twice. The first notice removes the
    // stand-in, so stopping the monitor afterwards has nothing to stop.
    proxy.send(Signal::ProcessDied(7));
    node_client.monitored_process_died(monitor);
    node_client.remove_monitor_proxy(monitor);
    assert!(monitored_signals.try_recv().is_err());

    // A stopped monitor doesn't notify.
    let (monitored, mut monitored_signals) = signal_recorder(8);
    let monitor = RemoteMonitor {
        process: ProcessId(8),
        ..monitor
    };
    let proxy = node_client.monitor_proxy(monitor, monitored);
    node_client.remove_monitor_proxy(monitor);
    match monitored_signals.try_recv() {
        Ok(Signal::StopMonitoring { process_id }) => assert_eq!(process_id, proxy.id()),
        signal => panic!("unexpected signal {signal:?}"),
    }
}
