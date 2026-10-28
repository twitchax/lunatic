use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::{Process, Signal};

#[async_trait]
pub trait Environment: Send + Sync {
    fn id(&self) -> u64;
    fn get_next_process_id(&self) -> u64;
    fn get_process(&self, id: u64) -> Option<Arc<dyn Process>>;
    fn add_process(&self, id: u64, proc: Arc<dyn Process>);
    fn remove_process(&self, id: u64);
    fn process_count(&self) -> usize;
    async fn can_spawn_next_process(&self) -> Result<Option<()>>;
    fn send(&self, id: u64, signal: Signal);
}

#[async_trait]
pub trait Environments: Send + Sync {
    type Env: Environment;

    /// Creates the environment with the given ID, or returns it if it already exists.
    async fn create(&self, id: u64) -> Result<Arc<Self::Env>>;
    async fn get(&self, id: u64) -> Option<Arc<Self::Env>>;
}

#[derive(Clone)]
pub struct LunaticEnvironment {
    environment_id: u64,
    next_process_id: Arc<AtomicU64>,
    processes: Arc<DashMap<u64, Arc<dyn Process>>>,
}

impl LunaticEnvironment {
    pub fn new(id: u64) -> Self {
        Self {
            environment_id: id,
            processes: Arc::new(DashMap::new()),
            next_process_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

#[async_trait]
impl Environment for LunaticEnvironment {
    fn get_process(&self, id: u64) -> Option<Arc<dyn Process>> {
        self.processes.get(&id).map(|x| x.clone())
    }

    fn add_process(&self, id: u64, proc: Arc<dyn Process>) {
        self.processes.insert(id, proc);
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels: [(String, String); 0] = [];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [("environment_id", self.id().to_string())];
        #[cfg(feature = "metrics")]
        metrics::gauge!("lunatic.process.environment.process.count", &labels)
            .set(self.processes.len() as f64);
    }

    fn remove_process(&self, id: u64) {
        self.processes.remove(&id);
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels: [(String, String); 0] = [];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [("environment_id", self.id().to_string())];
        #[cfg(feature = "metrics")]
        metrics::gauge!("lunatic.process.environment.process.count", &labels)
            .set(self.processes.len() as f64);
    }

    fn process_count(&self) -> usize {
        self.processes.len()
    }

    fn send(&self, id: u64, signal: Signal) {
        if let Some(proc) = self.processes.get(&id) {
            proc.send(signal);
        }
    }

    fn get_next_process_id(&self) -> u64 {
        self.next_process_id.fetch_add(1, Ordering::Relaxed)
    }

    fn id(&self) -> u64 {
        self.environment_id
    }

    async fn can_spawn_next_process(&self) -> Result<Option<()>> {
        // Don't impose any limits to process spawning
        Ok(Some(()))
    }
}

#[derive(Clone, Default)]
pub struct LunaticEnvironments {
    envs: Arc<DashMap<u64, Arc<LunaticEnvironment>>>,
}

#[async_trait]
impl Environments for LunaticEnvironments {
    type Env = LunaticEnvironment;
    async fn create(&self, id: u64) -> Result<Arc<Self::Env>> {
        // Nodes create environments on demand, e.g. when processes are spawned into them from
        // other nodes, concurrently. Replacing an environment would make its processes unreachable.
        let env = self
            .envs
            .entry(id)
            .or_insert_with(|| Arc::new(LunaticEnvironment::new(id)))
            .clone();
        #[cfg(feature = "metrics")]
        metrics::gauge!("lunatic.process.environment.count").set(self.envs.len() as f64);
        Ok(env)
    }

    async fn get(&self, id: u64) -> Option<Arc<Self::Env>> {
        self.envs.get(&id).map(|e| e.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creating_an_existing_environment_returns_it() {
        let envs = LunaticEnvironments::default();
        let env = envs.create(1).await.unwrap();
        assert!(Arc::ptr_eq(&env, &envs.create(1).await.unwrap()));
        assert!(Arc::ptr_eq(&env, &envs.get(1).await.unwrap()));
        assert!(!Arc::ptr_eq(&env, &envs.create(2).await.unwrap()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrently_created_environments_are_the_same_environment() {
        let envs = LunaticEnvironments::default();
        let creating: Vec<_> = (0..16)
            .map(|_| {
                let envs = envs.clone();
                tokio::spawn(async move { envs.create(1).await.unwrap() })
            })
            .collect();
        for created in creating {
            assert!(Arc::ptr_eq(
                &created.await.unwrap(),
                &envs.get(1).await.unwrap()
            ));
        }
    }
}
