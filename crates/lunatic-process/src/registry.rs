//! Names of processes, shared by the processes spawned from the same process.
//!
//! `get_or_spawn` reserves a name while it spawns the process, without holding the registry's
//! lock across the spawn, which may wait on another node. Meanwhile:
//! * `get` doesn't find the name,
//! * `put`, `remove` and other spawns under the name wait until the spawn finished: `put` then
//!   replaces the spawned process, `remove` removes it, and a spawn gets it.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::sync::watch;

#[derive(Default)]
pub struct Registry {
    entries: Mutex<HashMap<String, Entry>>,
}

enum Entry {
    // The node and process IDs.
    Registered(u64, u64),
    // A process is being spawned under the name. Wakes up waiters when the reservation is
    // dropped.
    Reserved(watch::Receiver<()>),
}

impl Registry {
    /// Returns the node and process IDs registered under `name`.
    pub fn get(&self, name: &str) -> Option<(u64, u64)> {
        match self.entries().get(name) {
            Some(Entry::Registered(node_id, process_id)) => Some((*node_id, *process_id)),
            Some(Entry::Reserved(_)) | None => None,
        }
    }

    /// Registers a process under `name`, replacing any other.
    pub async fn put(&self, name: &str, node_id: u64, process_id: u64) {
        let mut entries = self.wait_for_spawn(name).await;
        entries.insert(name.to_owned(), Entry::Registered(node_id, process_id));
    }

    /// Removes the process registered under `name`.
    pub async fn remove(&self, name: &str) {
        self.wait_for_spawn(name).await.remove(name);
    }

    /// Reserves `name` for a process that's going to be spawned, unless a process is registered
    /// under it: then returns its node and process IDs.
    pub async fn reserve(self: &Arc<Self>, name: &str) -> Result<Reservation, (u64, u64)> {
        let mut entries = self.wait_for_spawn(name).await;
        if let Some(Entry::Registered(node_id, process_id)) = entries.get(name) {
            return Err((*node_id, *process_id));
        }
        let (spawned, reserved) = watch::channel(());
        entries.insert(name.to_owned(), Entry::Reserved(reserved));
        Ok(Reservation {
            registry: self.clone(),
            name: name.to_owned(),
            _spawned: spawned,
        })
    }

    /// The names that the process `process_id` is registered under.
    pub fn names(&self, process_id: u64) -> Vec<String> {
        self.entries()
            .iter()
            .filter(|(_, entry)| matches!(entry, Entry::Registered(_, id) if *id == process_id))
            .map(|(name, _)| name.clone())
            .collect()
    }

    // Locks the entries once `name` isn't reserved.
    async fn wait_for_spawn(&self, name: &str) -> MutexGuard<'_, HashMap<String, Entry>> {
        loop {
            let mut spawned = {
                let entries = self.entries();
                match entries.get(name) {
                    Some(Entry::Reserved(spawned)) => spawned.clone(),
                    _ => return entries,
                }
            };
            // Errs once the reservation is dropped, and only then.
            spawned.changed().await.ok();
        }
    }

    fn entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        // Entries are only replaced while locked, so they're consistent even after a panic.
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A name reserved for a process that's being spawned. Dropping it releases the name.
pub struct Reservation {
    registry: Arc<Registry>,
    name: String,
    // Dropped after the entry was updated, which wakes up the waiters.
    _spawned: watch::Sender<()>,
}

impl Reservation {
    /// Registers the spawned process under the reserved name.
    pub fn register(self, node_id: u64, process_id: u64) {
        self.registry
            .entries()
            .insert(self.name.clone(), Entry::Registered(node_id, process_id));
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut entries = self.registry.entries();
        if let Some(Entry::Reserved(_)) = entries.get(&self.name) {
            entries.remove(&self.name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn reserved_names_are_not_found() {
        let registry = Arc::new(Registry::default());
        let reservation = registry.reserve("name").await.unwrap();
        assert_eq!(registry.get("name"), None);
        reservation.register(1, 2);
        assert_eq!(registry.get("name"), Some((1, 2)));
        assert_eq!(registry.names(2), vec!["name".to_owned()]);
    }

    #[tokio::test]
    async fn released_names_are_free() {
        let registry = Arc::new(Registry::default());
        drop(registry.reserve("name").await.unwrap());
        assert_eq!(registry.get("name"), None);
        assert!(registry.reserve("name").await.is_ok());
    }

    #[tokio::test]
    async fn registered_names_are_not_reserved() {
        let registry = Arc::new(Registry::default());
        registry.put("name", 1, 2).await;
        assert_eq!(registry.reserve("name").await.err(), Some((1, 2)));
    }

    #[tokio::test]
    async fn spawns_under_a_reserved_name_get_the_spawned_process() {
        let registry = Arc::new(Registry::default());
        let reservation = registry.reserve("name").await.unwrap();
        let waiting = tokio::spawn({
            let registry = registry.clone();
            async move { registry.reserve("name").await.err() }
        });
        // The test runtime is single-threaded, so the task runs until it waits.
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        reservation.register(1, 2);
        assert_eq!(waiting.await.unwrap(), Some((1, 2)));
    }

    #[tokio::test]
    async fn spawns_under_a_released_name_reserve_it() {
        let registry = Arc::new(Registry::default());
        let reservation = registry.reserve("name").await.unwrap();
        let waiting = tokio::spawn({
            let registry = registry.clone();
            async move { registry.reserve("name").await.is_ok() }
        });
        // The test runtime is single-threaded, so the task runs until it waits.
        tokio::task::yield_now().await;
        drop(reservation);
        assert!(waiting.await.unwrap());
    }

    #[tokio::test]
    async fn puts_and_removes_wait_for_the_spawn() {
        let registry = Arc::new(Registry::default());
        let reservation = registry.reserve("name").await.unwrap();
        let putting = tokio::spawn({
            let registry = registry.clone();
            async move { registry.put("name", 3, 4).await }
        });
        // The test runtime is single-threaded, so the task runs until it waits.
        tokio::task::yield_now().await;
        assert!(!putting.is_finished());
        reservation.register(1, 2);
        putting.await.unwrap();
        assert_eq!(registry.get("name"), Some((3, 4)));

        let reservation = registry.reserve("other").await.unwrap();
        let removing = tokio::spawn({
            let registry = registry.clone();
            async move { registry.remove("other").await }
        });
        // The test runtime is single-threaded, so the task runs until it waits.
        tokio::task::yield_now().await;
        assert!(!removing.is_finished());
        reservation.register(1, 2);
        removing.await.unwrap();
        assert_eq!(registry.get("other"), None);
    }
}
