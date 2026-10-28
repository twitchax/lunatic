use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::{
        atomic::{self, AtomicU64},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Result;
use axum::{Extension, Router};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use lunatic_control::api::{NodeStart, Register};
use rcgen::{Issuer, KeyPair};
use uuid::Uuid;

use crate::routes;

/// How long a node stays in the cluster without contacting the control server. Nodes renew their
/// lease with every request, and refresh their node list every second.
pub const NODE_LEASE: Duration = Duration::from_secs(5);

// The `status` of a node.
pub const NODE_STARTED: i16 = 0;
pub const NODE_STOPPED: i16 = 2;
// A node whose lease ran out. Its later requests are rejected for good.
pub const NODE_EXPIRED: i16 = 3;

/// The lease of a node ran out.
#[derive(Debug)]
pub struct LeaseExpired;

pub struct ControlServer {
    pub ca_cert: Issuer<'static, KeyPair>,
    pub quic_client: lunatic_distributed::quic::Client,
    pub registrations: DashMap<u64, Registered>,
    pub nodes: DashMap<u64, NodeDetails>,
    pub modules: DashMap<u64, Vec<u8>>,
    next_registration_id: AtomicU64,
    next_node_id: AtomicU64,
    next_module_id: AtomicU64,
    lease: Duration,
}

#[derive(Clone)]
pub struct Registered {
    pub node_name: Uuid,
    pub csr_pem: String,
    pub cert_pem: String,
    pub authentication_token: String,
}

pub struct NodeDetails {
    pub registration_id: u64,
    pub status: i16,
    pub created_at: DateTime<Utc>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub node_address: String,
    pub attributes: HashMap<String, String>,
    // When the node last contacted the control server.
    pub last_seen: Instant,
}

impl ControlServer {
    pub fn new(
        ca_cert: Issuer<'static, KeyPair>,
        quic_client: lunatic_distributed::quic::Client,
    ) -> Self {
        Self {
            ca_cert,
            quic_client,
            registrations: DashMap::new(),
            nodes: DashMap::new(),
            modules: DashMap::new(),
            next_registration_id: AtomicU64::new(1),
            next_node_id: AtomicU64::new(1),
            next_module_id: AtomicU64::new(1),
            lease: NODE_LEASE,
        }
    }

    /// A control server with the test root certificate.
    pub fn with_test_root_cert() -> Result<Self> {
        let ca_cert_str = lunatic_distributed::distributed::server::test_root_cert();
        let ca_cert = lunatic_distributed::control::cert::test_root_cert()?;
        let (ctrl_cert, ctrl_pk) =
            lunatic_distributed::control::cert::default_server_certificates(&ca_cert)?;
        let quic_client =
            lunatic_distributed::quic::new_quic_client(&ca_cert_str, &ctrl_cert, &ctrl_pk)?;
        Ok(Self::new(ca_cert, quic_client))
    }

    /// Sets how long nodes stay in the cluster without contacting the control server.
    pub fn with_lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    pub fn register(&self, reg: &Register, cert_pem: &str, authentication_token: &str) {
        let id = self
            .next_registration_id
            .fetch_add(1, atomic::Ordering::Relaxed);
        let registered = Registered {
            node_name: reg.node_name,
            csr_pem: reg.csr_pem.clone(),
            cert_pem: cert_pem.to_owned(),
            authentication_token: authentication_token.to_owned(),
        };
        self.registrations.insert(id, registered);
    }

    pub fn start_node(&self, registration_id: u64, data: NodeStart) -> (u64, String) {
        let id = self.next_node_id.fetch_add(1, atomic::Ordering::Relaxed);
        let details = NodeDetails {
            registration_id,
            status: 0,
            created_at: Utc::now(),
            stopped_at: None,
            node_address: data.node_address.to_string(),
            attributes: data.attributes,
            last_seen: Instant::now(),
        };
        self.nodes.insert(id, details);
        (id, data.node_address.to_string())
    }

    /// Stops the started node of a registration.
    pub fn stop_node(&self, registration_id: u64) {
        for mut node in self.nodes.iter_mut() {
            if node.registration_id == registration_id && node.status == NODE_STARTED {
                node.status = NODE_STOPPED;
                node.stopped_at = Some(Utc::now());
            }
        }
    }

    /// Renews the lease of the registration's started node, unless the lease already ran out.
    pub fn renew_lease(&self, registration_id: u64) -> Result<(), LeaseExpired> {
        let mut result = Ok(());
        for mut node in self.nodes.iter_mut() {
            if node.registration_id != registration_id {
                continue;
            }
            if node.status == NODE_STARTED && node.last_seen.elapsed() > self.lease {
                node.status = NODE_EXPIRED;
                node.stopped_at = Some(Utc::now());
            }
            match node.status {
                NODE_STARTED => node.last_seen = Instant::now(),
                NODE_EXPIRED => result = Err(LeaseExpired),
                _ => {}
            }
        }
        result
    }

    /// Whether a node is part of the cluster: started, and with a lease that didn't run out.
    pub fn is_live(&self, node: &NodeDetails) -> bool {
        node.status == NODE_STARTED && node.last_seen.elapsed() <= self.lease
    }

    pub fn add_module(&self, bytes: Vec<u8>) -> u64 {
        let id = self.next_module_id.fetch_add(1, atomic::Ordering::Relaxed);
        self.modules.insert(id, bytes);
        id
    }
}

pub async fn control_server(http_socket: SocketAddr) -> Result<()> {
    control_server_from_tcp(TcpListener::bind(http_socket)?).await
}

pub async fn control_server_from_tcp(listener: TcpListener) -> Result<()> {
    serve(listener, Arc::new(ControlServer::with_test_root_cert()?)).await
}

/// Serves the control API of `control`.
pub async fn serve(listener: TcpListener, control: Arc<ControlServer>) -> Result<()> {
    let app = Router::new()
        .merge(routes::init_routes())
        .layer(Extension(control));

    listener.set_nonblocking(true)?;
    axum::serve(tokio::net::TcpListener::from_std(listener)?, app).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Registers and starts a node, returning its registration and node IDs.
    fn start_node(control: &ControlServer) -> (u64, u64) {
        let register = Register {
            node_name: Uuid::new_v4(),
            csr_pem: String::new(),
        };
        control.register(&register, "", "token");
        let registration_id = *control
            .registrations
            .iter()
            .find(|registration| registration.node_name == register.node_name)
            .unwrap()
            .key();
        let start = NodeStart {
            node_address: "127.0.0.1:1".parse().unwrap(),
            attributes: HashMap::new(),
        };
        let (node_id, _) = control.start_node(registration_id, start);
        (registration_id, node_id)
    }

    #[tokio::test]
    async fn stopping_a_registration_stops_its_node() {
        let control = ControlServer::with_test_root_cert().unwrap();
        // Registration and node IDs differ.
        let (_, other_node_id) = start_node(&control);
        control.register(
            &Register {
                node_name: Uuid::new_v4(),
                csr_pem: String::new(),
            },
            "",
            "token",
        );
        let (registration_id, node_id) = start_node(&control);
        assert_ne!(registration_id, node_id);

        control.stop_node(registration_id);
        assert_eq!(control.nodes.get(&node_id).unwrap().status, NODE_STOPPED);
        assert_eq!(
            control.nodes.get(&other_node_id).unwrap().status,
            NODE_STARTED
        );
    }

    #[tokio::test]
    async fn nodes_expire_without_renewing_their_lease() {
        let control = ControlServer::with_test_root_cert().unwrap();
        let (registration_id, node_id) = start_node(&control);
        assert!(control.renew_lease(registration_id).is_ok());
        assert!(control.is_live(&control.nodes.get(&node_id).unwrap()));

        control.nodes.get_mut(&node_id).unwrap().last_seen = Instant::now() - NODE_LEASE * 2;
        assert!(!control.is_live(&control.nodes.get(&node_id).unwrap()));
        // Expiry is final.
        assert!(control.renew_lease(registration_id).is_err());
        assert!(control.renew_lease(registration_id).is_err());
        assert_eq!(control.nodes.get(&node_id).unwrap().status, NODE_EXPIRED);
    }

    #[tokio::test]
    async fn stopped_nodes_dont_expire() {
        let control = ControlServer::with_test_root_cert().unwrap();
        let (registration_id, node_id) = start_node(&control);
        control.stop_node(registration_id);
        control.nodes.get_mut(&node_id).unwrap().last_seen = Instant::now() - NODE_LEASE * 2;

        assert!(control.renew_lease(registration_id).is_ok());
        assert_eq!(control.nodes.get(&node_id).unwrap().status, NODE_STOPPED);
    }
}
