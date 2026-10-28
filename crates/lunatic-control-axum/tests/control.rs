//! A node talking to the control server over HTTP, the way `lunatic node` does.

use std::{collections::HashMap, net::TcpListener, sync::Arc, time::Duration};

use lunatic_control::api::{NodeStart, Registration};
use lunatic_control_axum::server::{control_server_from_tcp, serve, ControlServer};
use lunatic_distributed::{control::Client, distributed::server::gen_node_cert};
use uuid::Uuid;

#[tokio::test]
async fn node_registers_and_shares_modules() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));

    let http_client = reqwest::Client::new();
    let node_name = Uuid::new_v4();
    let (csr, _key) = gen_node_cert(&node_name.to_string()).unwrap();
    let reg = Client::register(
        &http_client,
        control_url.parse().unwrap(),
        node_name,
        csr.pem().unwrap(),
    )
    .await
    .unwrap();
    // The control's URLs are derived from the host the node reached it at.
    assert!(reg.urls.nodes.starts_with(&control_url));
    assert_eq!(reg.cert_pem_chain.len(), 1);

    let node_address = "[::1]:3031".parse().unwrap();
    let client = Client::new(http_client, reg, node_address, HashMap::new())
        .await
        .unwrap();
    assert_eq!(client.node_ids(), vec![client.node_id()]);
    assert_eq!(
        client.node_info(client.node_id()).unwrap().address,
        node_address
    );

    let module = client.add_module(vec![0, 97, 115, 109]).await.unwrap();
    let module_id = module.id.unwrap();
    assert_eq!(
        client.get_module(module_id, 1).await.unwrap(),
        vec![0, 97, 115, 109]
    );
    assert!(client.get_module(module_id + 1, 1).await.is_err());

    client.notify_node_stopped().await.unwrap();
    client.refresh_nodes().await.unwrap();
    assert!(client.node_ids().is_empty());
}

#[tokio::test]
async fn requests_without_node_credentials_are_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));

    let response = reqwest::get(format!("{control_url}nodes")).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body = response.text().await.unwrap();
    assert!(body.contains("no_auth_header"), "{body}");
}

// Sends a request authenticated as the registered node.
fn as_node(request: reqwest::RequestBuilder, reg: &Registration) -> reqwest::RequestBuilder {
    request.bearer_auth(&reg.authentication_token).header(
        "x-lunatic-node-name",
        reg.node_name.hyphenated().to_string(),
    )
}

#[tokio::test]
async fn nodes_whose_lease_ran_out_are_rejected() {
    const LEASE: Duration = Duration::from_millis(200);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    let control = ControlServer::with_test_root_cert()
        .unwrap()
        .with_lease(LEASE);
    tokio::spawn(serve(listener, Arc::new(control)));

    let http_client = reqwest::Client::new();
    let node_name = Uuid::new_v4();
    let (csr, _key) = gen_node_cert(&node_name.to_string()).unwrap();
    let reg = Client::register(
        &http_client,
        control_url.parse().unwrap(),
        node_name,
        csr.pem().unwrap(),
    )
    .await
    .unwrap();
    let start = NodeStart {
        node_address: "127.0.0.1:1".parse().unwrap(),
        attributes: HashMap::new(),
    };
    let started = as_node(http_client.post(&reg.urls.node_started), &reg)
        .json(&start)
        .send()
        .await
        .unwrap();
    assert!(started.status().is_success());
    let nodes = as_node(http_client.get(&reg.urls.nodes), &reg)
        .send()
        .await
        .unwrap();
    assert!(nodes.text().await.unwrap().contains("127.0.0.1:1"));

    // The node doesn't contact the control server for longer than its lease.
    tokio::time::sleep(LEASE * 2).await;
    for _ in 0..2 {
        let nodes = as_node(http_client.get(&reg.urls.nodes), &reg)
            .send()
            .await
            .unwrap();
        assert_eq!(nodes.status(), reqwest::StatusCode::GONE);
        assert!(nodes.text().await.unwrap().contains("node_expired"));
    }
}

// Registers and starts a node with the control server at `control_url`.
async fn start_node(control_url: &str) -> Client {
    let http_client = reqwest::Client::new();
    let node_name = Uuid::new_v4();
    let (csr, _key) = gen_node_cert(&node_name.to_string()).unwrap();
    let reg = Client::register(
        &http_client,
        control_url.parse().unwrap(),
        node_name,
        csr.pem().unwrap(),
    )
    .await
    .unwrap();
    let node_address = "127.0.0.1:1".parse().unwrap();
    Client::new(http_client, reg, node_address, HashMap::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn nodes_whose_lease_ran_out_are_fenced() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    // Shorter than the interval at which nodes renew their lease.
    let lease = Duration::from_millis(300);
    let control = ControlServer::with_test_root_cert()
        .unwrap()
        .with_lease(lease);
    tokio::spawn(serve(listener, Arc::new(control)));

    let client = start_node(&control_url).await;
    tokio::time::timeout(Duration::from_secs(5), client.fenced())
        .await
        .expect("the node wasn't fenced");
}

#[tokio::test]
async fn nodes_that_leave_are_reported() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    tokio::spawn(control_server_from_tcp(listener));

    let leaving = start_node(&control_url).await;
    let staying = start_node(&control_url).await;
    staying.refresh_nodes().await.unwrap();
    assert_eq!(staying.node_ids().len(), 2);
    let (down_sender, mut down) = tokio::sync::mpsc::unbounded_channel();
    staying.on_node_down(move |node_id| {
        down_sender.send(node_id).ok();
    });

    leaving.notify_node_stopped().await.unwrap();
    let down_node = tokio::time::timeout(Duration::from_secs(5), down.recv())
        .await
        .expect("the node wasn't reported down");
    assert_eq!(down_node, Some(leaving.node_id()));
    assert_eq!(staying.node_ids(), vec![staying.node_id()]);
}

#[tokio::test]
async fn nodes_keep_their_view_while_the_control_server_is_unreachable() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let control_url = format!("http://{}/", listener.local_addr().unwrap());
    let control_server = tokio::spawn(control_server_from_tcp(listener));

    let other = start_node(&control_url).await;
    let client = start_node(&control_url).await;
    client.refresh_nodes().await.unwrap();
    control_server.abort();

    assert!(client.refresh_nodes().await.is_err());
    let mut node_ids = client.node_ids();
    node_ids.sort();
    assert_eq!(node_ids, vec![other.node_id(), client.node_id()]);
    assert!(
        tokio::time::timeout(Duration::from_millis(100), client.fenced())
            .await
            .is_err()
    );
}
