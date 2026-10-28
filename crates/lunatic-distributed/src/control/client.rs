use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use lunatic_control::api::*;
use lunatic_control::NodeInfo;
use lunatic_process::runtimes::RawWasm;
use reqwest::{Client as HttpClient, Response, StatusCode, Url};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{atomic, atomic::AtomicU64, Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::watch;

// How often the node list is refreshed, which also renews this node's lease on the control server.
const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

type NodeDownHandler = Box<dyn Fn(u64) + Send + Sync>;

#[derive(Clone)]
pub struct Client {
    inner: Arc<InnerClient>,
}

pub struct InnerClient {
    reg: Registration,
    node_id: u64,
    http_client: HttpClient,
    next_message_id: AtomicU64,
    next_query_id: AtomicU64,
    node_queries: DashMap<u64, Vec<u64>>,
    nodes: DashMap<u64, NodeInfo>,
    node_ids: RwLock<Vec<u64>>,
    // Serializes node list refreshes, so that an older list never replaces a newer one.
    refreshing: tokio::sync::Mutex<()>,
    node_down_handlers: Mutex<Vec<NodeDownHandler>>,
    // Set once the control server dropped this node from the cluster.
    fenced: watch::Sender<bool>,
}

impl Client {
    pub async fn new(
        http_client: HttpClient,
        reg: Registration,
        node_address: SocketAddr,
        attributes: HashMap<String, String>,
    ) -> Result<Self> {
        let node_id = Self::start(
            &http_client,
            &reg,
            NodeStart {
                node_address,
                attributes,
            },
        )
        .await?;

        let client = Client {
            inner: Arc::new(InnerClient {
                reg,
                node_id,
                http_client,
                next_message_id: AtomicU64::new(1),
                node_queries: DashMap::new(),
                next_query_id: AtomicU64::new(1),
                nodes: Default::default(),
                node_ids: Default::default(),
                refreshing: Default::default(),
                node_down_handlers: Default::default(),
                fenced: watch::Sender::new(false),
            }),
        };

        tokio::task::spawn(refresh_nodes_task(client.clone()));
        client.refresh_nodes().await?;

        Ok(client)
    }

    pub async fn register(
        http_client: &HttpClient,
        control_url: Url,
        node_name: uuid::Uuid,
        csr_pem: String,
    ) -> Result<Registration> {
        let reg = Register { node_name, csr_pem };
        Self::send_registration(http_client, control_url, reg).await
    }

    pub fn reg(&self) -> Registration {
        self.inner.reg.clone()
    }

    pub fn node_id(&self) -> u64 {
        self.inner.node_id
    }

    pub fn next_message_id(&self) -> u64 {
        self.inner
            .next_message_id
            .fetch_add(1, atomic::Ordering::Relaxed)
    }

    pub fn next_query_id(&self) -> u64 {
        self.inner
            .next_query_id
            .fetch_add(1, atomic::Ordering::Relaxed)
    }

    async fn send_registration(
        client: &HttpClient,
        url: Url,
        reg: Register,
    ) -> Result<Registration> {
        let resp = client
            .post(url)
            .json(&reg)
            .send()
            .await
            .with_context(|| "Error sending HTTP registration request.")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.with_context(|| {
                format!("Error parsing body as a text. Response not successful: {status}")
            })?;
            Err(anyhow!(
                "HTTP registration request returned an error response: {body}"
            ))
        } else {
            let reg = resp
                .json()
                .await
                .with_context(|| "Error parsing the registration request JSON.")?;

            Ok(reg)
        }
    }

    async fn start(client: &HttpClient, reg: &Registration, start: NodeStart) -> Result<u64> {
        let resp: NodeStarted = client
            .post(&reg.urls.node_started)
            .json(&start)
            .bearer_auth(&reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await?
            .json()
            .await?;
        Ok(resp.node_id as u64)
    }

    pub async fn get<T: DeserializeOwned>(&self, url: &str, query: Option<&str>) -> Result<T> {
        let mut url: Url = url.parse()?;
        url.set_query(query);

        let resp: T = self
            .inner
            .http_client
            .get(url.clone())
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP GET request: {}.", url))
            .and_then(|response| self.check_lease(response))?
            .error_for_status()
            .with_context(|| format!("HTTP GET request returned an error response: {}", url))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP GET request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn post<T: Serialize, R: DeserializeOwned>(&self, url: &str, data: T) -> Result<R> {
        let url: Url = url.parse()?;

        let resp: R = self
            .inner
            .http_client
            .post(url.clone())
            .json(&data)
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP POST request: {}.", url))
            .and_then(|response| self.check_lease(response))?
            .error_for_status()
            .with_context(|| format!("HTTP POST request returned an error response: {}", url))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP POST request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn upload<R: DeserializeOwned>(&self, url: &str, body: Vec<u8>) -> Result<R> {
        let url: Url = url.parse()?;

        let resp: R = self
            .inner
            .http_client
            .post(url.clone())
            .body(body)
            .bearer_auth(&self.inner.reg.authentication_token)
            .header(
                "x-lunatic-node-name",
                &self.inner.reg.node_name.hyphenated().to_string(),
            )
            .send()
            .await
            .with_context(|| format!("Error sending HTTP POST request: {}.", url))
            .and_then(|response| self.check_lease(response))?
            .error_for_status()
            .with_context(|| format!("HTTP POST request returned an error response: {}", url))
            .map_err(|e| dbg!(e))?
            .json()
            .await
            .with_context(|| format!("Error parsing the HTTP POST request JSON: {}", url))?;

        Ok(resp)
    }

    pub async fn refresh_nodes(&self) -> Result<()> {
        let _refreshing = self.inner.refreshing.lock().await;
        let resp: NodesList = self.get(&self.inner.reg.urls.nodes, None).await?;
        let mut node_ids = vec![];
        for node in resp.nodes {
            let id = node.id;
            node_ids.push(id);
            if !self.inner.nodes.contains_key(&id) {
                self.inner.nodes.insert(id, node);
            }
        }
        let down: Vec<u64> = match self.inner.node_ids.write() {
            Ok(mut self_node_ids) => {
                let down = self_node_ids
                    .iter()
                    .filter(|id| !node_ids.contains(id))
                    .copied()
                    .collect();
                *self_node_ids = node_ids;
                down
            }
            Err(_) => Vec::new(),
        };
        // Nodes leave the list when they stop, or when their lease on the control server ran out.
        for node_id in down {
            log::warn!("Node {node_id} left the cluster");
            if let Ok(handlers) = self.inner.node_down_handlers.lock() {
                for handler in handlers.iter() {
                    handler(node_id);
                }
            }
        }
        Ok(())
    }

    /// Calls `handler` with the ID of every node that leaves the cluster.
    pub fn on_node_down(&self, handler: impl Fn(u64) + Send + Sync + 'static) {
        if let Ok(mut handlers) = self.inner.node_down_handlers.lock() {
            handlers.push(Box::new(handler));
        }
    }

    /// Returns once the control server dropped this node from the cluster, because its lease
    /// ran out. The node can't rejoin then.
    pub async fn fenced(&self) {
        let mut fenced = self.inner.fenced.subscribe();
        fenced.wait_for(|fenced| *fenced).await.ok();
    }

    // The control server rejects every request of a node whose lease ran out.
    fn check_lease(&self, response: Response) -> Result<Response> {
        if response.status() == StatusCode::GONE {
            self.inner.fenced.send_replace(true);
            return Err(anyhow!(
                "The control server dropped this node from the cluster, its lease ran out"
            ));
        }
        Ok(response)
    }

    pub async fn notify_node_stopped(&self) -> Result<()> {
        self.post::<_, ()>(&self.inner.reg.urls.node_stopped, ())
            .await?;
        Ok(())
    }

    pub fn node_info(&self, node_id: u64) -> Option<NodeInfo> {
        self.inner.nodes.get(&node_id).map(|e| e.clone())
    }

    pub fn node_ids(&self) -> Vec<u64> {
        self.inner.node_ids.read().unwrap().clone()
    }

    pub async fn lookup_nodes(&self, query: &str) -> Result<(u64, usize)> {
        let resp: NodesList = self
            .get(&self.inner.reg.urls.get_nodes, Some(query))
            .await?;
        let nodes: Vec<u64> = resp.nodes.into_iter().map(move |v| v.id).collect();
        let nodes_count = nodes.len();
        let query_id = self.next_query_id();
        self.inner.node_queries.insert(query_id, nodes);
        Ok((query_id, nodes_count))
    }

    pub fn query_result(&self, query_id: &u64) -> Option<(u64, Vec<u64>)> {
        self.inner.node_queries.remove(query_id)
    }

    pub fn node_count(&self) -> usize {
        self.inner.node_ids.read().unwrap().len()
    }

    pub async fn get_module(&self, module_id: u64, environment_id: u64) -> Result<Vec<u8>> {
        log::info!("Get module {module_id}");
        let url = self
            .inner
            .reg
            .urls
            .get_module
            .replace("{id}", &module_id.to_string());
        let query = format!("env_id={environment_id}");
        let resp: ModuleBytes = self.get(&url, Some(&query)).await?;
        Ok(resp.bytes)
    }

    pub async fn add_module(&self, module: Vec<u8>) -> Result<RawWasm> {
        let url = &self.inner.reg.urls.add_module;
        let resp: ModuleId = self.upload(url, module.clone()).await?;
        Ok(RawWasm::new(Some(resp.module_id), module))
    }
}

// When the control server can't be reached, the node keeps its last known node list.
async fn refresh_nodes_task(client: Client) -> Result<()> {
    loop {
        client.refresh_nodes().await.ok();
        tokio::time::sleep(REFRESH_INTERVAL).await;
    }
}
