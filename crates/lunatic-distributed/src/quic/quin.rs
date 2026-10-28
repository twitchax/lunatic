use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use dashmap::DashMap;
use lunatic_process::{env::Environment, state::ProcessState};
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    ClientConfig, Connection, ConnectionError, Endpoint, IdleTimeout, Incoming, ServerConfig,
    TransportConfig, VarInt,
};
use rustls::{
    crypto::{ring, CryptoProvider},
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
use wasmtime::ResourceLimiter;
use x509_parser::{der_parser::oid, oid_registry::asn1_rs::Utf8String, prelude::FromDer};

use crate::{
    distributed::{self},
    CertAttrs, DistributedCtx,
};

#[derive(Clone)]
pub struct Client {
    inner: Endpoint,
}

impl Client {
    pub async fn _connect(&self, addr: SocketAddr, name: &str) -> Result<quinn::Connection> {
        Ok(self.inner.connect(addr, name)?.await?)
    }

    pub async fn try_connect(
        &self,
        addr: SocketAddr,
        name: &str,
        retry: u32,
    ) -> Result<quinn::Connection> {
        for try_num in 1..(retry + 1) {
            match self._connect(addr, name).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    log::error!("Error connecting to {name} at {addr}, try {try_num}. Error: {e}")
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(anyhow!("Failed to connect to {name} at {addr}"))
    }
}

fn get_cert_attrs(conn: &Connection) -> Result<CertAttrs> {
    let peer_identity = match conn
        .peer_identity()
        .ok_or(anyhow!("Peer must provide an identity."))?
        .downcast::<Vec<CertificateDer<'static>>>()
    {
        Ok(certs) => Ok(certs),
        Err(_) => Err(anyhow!("Failed to downcast peer identity.")),
    }?;
    if peer_identity.len() != 1 {
        return Err(anyhow!("More than one identity certificate detected."));
    }
    let cert = peer_identity.first().unwrap();
    let (_rem, x509) = x509_parser::certificate::X509Certificate::from_der(cert)?;
    let oid = oid!(2.5.29 .9);
    let ext = x509
        .get_extension_unique(&oid)?
        .ok_or_else(|| anyhow!("Missing critical Lunatic certificate extension."))?;
    let (_rem, value) = Utf8String::from_der(ext.value)?;
    Ok(serde_json::from_str(&value.string())?)
}

// The crypto provider of all QUIC connections (rustls' former default, *ring*).
fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

// quinn 0.11 raised the default idle timeout from 10s to 30s. Keep 10s, so that dead peers are
// detected as quickly as before.
fn max_idle_timeout() -> IdleTimeout {
    IdleTimeout::from(VarInt::from_u32(10_000))
}

fn parse_cert(pem: &str) -> Result<CertificateDer<'static>> {
    CertificateDer::from_pem_slice(pem.as_bytes()).map_err(|_| anyhow!("Not a valid certificate."))
}

fn parse_private_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    let key = PrivatePkcs8KeyDer::from_pem_slice(pem.as_bytes())
        .map_err(|_| anyhow!("Not a valid private key."))?;
    Ok(key.into())
}

pub fn new_quic_client(ca_cert: &str, cert: &str, key: &str) -> Result<Client> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(parse_cert(ca_cert)?)?;

    let pk = parse_private_key(key)?;
    let cert = vec![parse_cert(cert)?];

    let client_crypto = rustls::ClientConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_client_auth_cert(cert, pk)?;

    let mut client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(Some(max_idle_timeout()));
    client_config.transport_config(Arc::new(transport));
    let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);
    Ok(Client { inner: endpoint })
}

pub fn new_quic_server(
    addr: SocketAddr,
    certs: Vec<String>,
    key: &str,
    ca_cert: &str,
) -> Result<Endpoint> {
    let server_config = quic_server_config(certs, key, ca_cert)?;
    Ok(quinn::Endpoint::server(server_config, addr)?)
}

pub(crate) fn quic_server_config(
    certs: Vec<String>,
    key: &str,
    ca_cert: &str,
) -> Result<ServerConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(parse_cert(ca_cert)?)?;

    let pk = parse_private_key(key)?;

    let mut cert_chain = Vec::new();
    for (i, cert) in certs.iter().enumerate() {
        let cert = parse_cert(cert)?;
        if i != 0 {
            roots.add(cert.clone())?;
        }
        cert_chain.push(cert);
    }

    let provider = crypto_provider();
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build()?;
    let server_crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, pk)?;
    let mut server_config =
        ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .keep_alive_interval(Some(Duration::from_millis(100)))
        .max_idle_timeout(Some(max_idle_timeout()));
    Ok(server_config)
}

pub async fn handle_node_server<T, E>(
    quic_server: &mut Endpoint,
    ctx: distributed::server::ServerCtx<T, E>,
) -> Result<()>
where
    T: ProcessState + ResourceLimiter + DistributedCtx<E> + Send + Sync + 'static,
    E: Environment + 'static,
{
    while let Some(incoming) = quic_server.accept().await {
        tokio::spawn(handle_quic_connection_node(ctx.clone(), incoming));
    }
    Err(anyhow!("Node server exited"))
}

pub struct NodeEnvPermission(pub Option<HashSet<u64>>);

impl NodeEnvPermission {
    fn new(cert_attrs: CertAttrs) -> Self {
        let some_set: Option<HashSet<u64>> = if cert_attrs.is_privileged {
            None
        } else {
            Some(cert_attrs.allowed_envs.into_iter().collect())
        };
        Self(some_set)
    }
}

async fn handle_quic_connection_node<T, E>(
    ctx: distributed::server::ServerCtx<T, E>,
    incoming: Incoming,
) -> Result<()>
where
    T: ProcessState + ResourceLimiter + DistributedCtx<E> + Send + Sync + 'static,
    E: Environment + 'static,
{
    log::info!("New node connection");
    let conn = incoming.await?;
    let node_cert_attrs = get_cert_attrs(&conn)?;
    let node_permissions = Arc::new(NodeEnvPermission::new(node_cert_attrs));
    log::info!("Remote {} connected", conn.remote_address());
    loop {
        if let Some(reason) = conn.close_reason() {
            log::info!("Connection {} is closed: {reason}", conn.remote_address());
            break;
        }
        let stream = conn.accept_uni().await;
        log::info!("Stream from remote {} accepted", conn.remote_address());
        match stream {
            Ok(recv) => {
                tokio::spawn(handle_quic_stream_node(
                    ctx.clone(),
                    recv,
                    node_permissions.clone(),
                ));
            }
            Err(ConnectionError::LocallyClosed) => {
                log::trace!("distributed::server::stream locally closed");
                break;
            }
            Err(_) => {}
        }
    }
    log::info!("Connection from remote {} closed", conn.remote_address());
    Ok(())
}

async fn handle_quic_stream_node<T, E>(
    ctx: distributed::server::ServerCtx<T, E>,
    recv: quinn::RecvStream,
    node_permissions: Arc<NodeEnvPermission>,
) where
    T: ProcessState + ResourceLimiter + DistributedCtx<E> + Send + Sync + 'static,
    E: Environment + 'static,
{
    let mut recv_ctx = RecvCtx {
        recv,
        chunks: DashMap::new(),
    };
    log::trace!("distributed::server::handle_quic_stream started");
    while let Ok((msg_id, bytes)) = read_next_stream_message(&mut recv_ctx).await {
        if let Ok(request) = rmp_serde::from_slice::<distributed::message::Request>(&bytes) {
            distributed::server::handle_message(
                ctx.clone(),
                msg_id,
                request,
                node_permissions.clone(),
            )
            .await;
        } else {
            log::debug!("Error deserializing request");
        }
    }
    log::trace!("distributed::server::handle_quic_stream finished");
}

pub(crate) struct Chunk {
    pub(crate) message_id: u64,
    message_size: usize,
    pub(crate) data: Vec<u8>,
}

struct RecvCtx {
    recv: quinn::RecvStream,
    // Map to collect message chunks key: message_id, data: (message_size, data)
    chunks: DashMap<u64, (usize, Vec<u8>)>,
}

pub(crate) async fn read_next_stream_chunk(recv: &mut quinn::RecvStream) -> Result<Chunk> {
    // Read chunk header info
    let mut message_id = [0u8; 8];
    let mut message_size = [0u8; 4];
    let mut chunk_id = [0u8; 8];
    let mut chunk_size = [0u8; 4];
    recv.read_exact(&mut message_id)
        .await
        .map_err(|e| anyhow!("{e} failed to read header message_id"))?;
    recv.read_exact(&mut message_size)
        .await
        .map_err(|e| anyhow!("{e} failed to read header message_size"))?;
    recv.read_exact(&mut chunk_id)
        .await
        .map_err(|e| anyhow!("{e} failed to read header chunk_id"))?;
    recv.read_exact(&mut chunk_size)
        .await
        .map_err(|e| anyhow!("{e} failed to read header chunk_size"))?;
    let message_id = u64::from_le_bytes(message_id);
    let message_size = u32::from_le_bytes(message_size) as usize;
    let chunk_id = u64::from_le_bytes(chunk_id);
    let chunk_size = u32::from_le_bytes(chunk_size) as usize;
    // Read chunk data
    let mut data = vec![0u8; chunk_size];
    recv.read_exact(&mut data)
        .await
        .map_err(|e| anyhow!("{e} failed to read message body"))?;
    log::trace!("read message_id={message_id} chunk_id={chunk_id}");
    Ok(Chunk {
        message_id,
        message_size,
        data,
    })
}

async fn read_next_stream_message(ctx: &mut RecvCtx) -> Result<(u64, Bytes)> {
    loop {
        let new_chunk = read_next_stream_chunk(&mut ctx.recv).await?;
        let message_id = new_chunk.message_id;
        let message_size = new_chunk.message_size;
        if let Some(mut entry) = ctx.chunks.get_mut(&message_id) {
            entry.1.extend(new_chunk.data);
        } else {
            ctx.chunks
                .insert(message_id, (message_size, new_chunk.data));
        };
        let finished = ctx
            .chunks
            .get(&message_id)
            .map(|entry| entry.0 == entry.1.len());
        match finished {
            Some(true) => {
                let (message_id, data) = ctx.chunks.remove(&message_id).unwrap();
                log::trace!("Finished collecting message_id={message_id}");
                return Ok((message_id, Bytes::from(data.1)));
            }
            Some(false) => {
                continue;
            }
            None => unreachable!("Message must exists at all times"),
        }
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateSigningRequestParams, CustomExtension};
    use x509_parser::oid_registry::asn1_rs::ToDer;

    use super::*;
    use crate::{
        control::cert::{test_root_cert, TEST_ROOT_CERT},
        distributed::server::gen_node_cert,
        SUBJECT_DIR_ATTRS,
    };

    // Issues a node certificate the way the control server does, returning it and its key.
    fn node_certificate(node_name: &str, attrs: &CertAttrs) -> (String, String) {
        let (csr, key) = gen_node_cert(node_name).unwrap();
        let mut csr = CertificateSigningRequestParams::from_pem(&csr.pem().unwrap()).unwrap();
        let attrs = serde_json::to_string(attrs).unwrap();
        csr.params
            .custom_extensions
            .push(CustomExtension::from_oid_content(
                &SUBJECT_DIR_ATTRS,
                attrs.to_der_vec().unwrap(),
            ));
        let cert = csr.signed_by(&test_root_cert().unwrap()).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    // Encodes one chunk of a message the way the congestion control's stream tasks do.
    fn chunk(message_id: u64, message_size: u32, chunk_id: u64, data: &[u8]) -> Vec<u8> {
        let mut chunk = Vec::new();
        chunk.extend(message_id.to_le_bytes());
        chunk.extend(message_size.to_le_bytes());
        chunk.extend(chunk_id.to_le_bytes());
        chunk.extend((data.len() as u32).to_le_bytes());
        chunk.extend(data);
        chunk
    }

    #[tokio::test]
    async fn nodes_exchange_messages_over_mutual_tls() {
        let attrs = CertAttrs {
            allowed_envs: vec![7],
            is_privileged: false,
        };
        let (cert, key) = node_certificate("node-a", &attrs);
        let server = new_quic_server(
            "[::1]:0".parse().unwrap(),
            vec![cert.clone()],
            &key,
            TEST_ROOT_CERT,
        )
        .unwrap();
        let client = new_quic_client(TEST_ROOT_CERT, &cert, &key).unwrap();

        let (client_conn, server_conn) = tokio::join!(
            client.try_connect(server.local_addr().unwrap(), "node-a", 1),
            async { server.accept().await.unwrap().await }
        );
        let client_conn = client_conn.unwrap();
        let server_conn = server_conn.unwrap();

        // The server reads the connecting node's permissions from its certificate.
        let peer_attrs = get_cert_attrs(&server_conn).unwrap();
        assert_eq!(peer_attrs.allowed_envs, attrs.allowed_envs);
        assert_eq!(peer_attrs.is_privileged, attrs.is_privileged);

        // A message sent in chunks arrives whole.
        let mut send = client_conn.open_uni().await.unwrap();
        send.write_all(&chunk(42, 11, 0, b"hello ")).await.unwrap();
        send.write_all(&chunk(42, 11, 1, b"world")).await.unwrap();
        let mut recv_ctx = RecvCtx {
            recv: server_conn.accept_uni().await.unwrap(),
            chunks: DashMap::new(),
        };
        let (message_id, data) = read_next_stream_message(&mut recv_ctx).await.unwrap();
        assert_eq!(message_id, 42);
        assert_eq!(&data[..], b"hello world");
    }

    #[tokio::test]
    async fn nodes_reject_certificates_from_other_cas() {
        let (cert, key) = node_certificate(
            "node-a",
            &CertAttrs {
                allowed_envs: vec![],
                is_privileged: true,
            },
        );
        let other_ca = rcgen::generate_simple_self_signed(vec!["other".to_string()]).unwrap();
        let server = new_quic_server(
            "[::1]:0".parse().unwrap(),
            vec![cert.clone()],
            &key,
            &other_ca.cert.pem(),
        )
        .unwrap();
        let client = new_quic_client(TEST_ROOT_CERT, &cert, &key).unwrap();

        let (client_conn, server_conn) = tokio::join!(
            client._connect(server.local_addr().unwrap(), "node-a"),
            async { server.accept().await.unwrap().await }
        );
        assert!(server_conn.is_err());
        // The client only learns about the rejection after its side of the handshake completed.
        if let Ok(client_conn) = client_conn {
            assert!(client_conn.accept_uni().await.is_err());
        }
    }
}
