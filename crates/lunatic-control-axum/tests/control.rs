//! A node talking to the control server over HTTP, the way `lunatic node` does.

use std::{collections::HashMap, net::TcpListener};

use lunatic_control_axum::server::control_server_from_tcp;
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
