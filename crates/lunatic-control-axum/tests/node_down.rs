//! A node's requests to another node fail once the other node leaves the cluster.

use std::{collections::HashMap, net::TcpListener, time::Duration};

use lunatic_control_axum::server::control_server_from_tcp;
use lunatic_distributed::{
    control,
    distributed::{
        self,
        client::{EnvironmentId, MessageId, NodeId, ProcessId, SpawnParams},
        message::{ClientError, ResponseContent, Spawn},
        server::gen_node_cert,
    },
    quic,
};
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
