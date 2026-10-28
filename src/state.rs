use std::fmt::Debug;
use std::sync::Arc;

use anyhow::Result;
use hash_map_id::HashMapId;
use lunatic_distributed::{
    distributed::client::{EnvironmentId, ProcessId},
    DistributedCtx, DistributedProcessState,
};
use lunatic_error_api::{ErrorCtx, ErrorResource};
use lunatic_networking_api::{DnsIterator, TlsConnection, TlsListener};
use lunatic_networking_api::{NetworkingCtx, TcpConnection};
use lunatic_process::env::{Environment, LunaticEnvironment};
use lunatic_process::runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime};
use lunatic_process::state::{ConfigResources, ProcessState};
use lunatic_process::{
    config::ProcessConfig,
    state::{SignalReceiver, SignalSender},
};
use lunatic_process::{mailbox::MessageMailbox, message::Message, registry::Registry};
use lunatic_process_api::{ProcessConfigCtx, ProcessCtx};
use lunatic_sqlite_api::{SQLiteConnections, SQLiteCtx, SQLiteGuestAllocators, SQLiteStatements};
use lunatic_stdout_capture::StdoutCapture;
use lunatic_timer_api::{TimerCtx, TimerResources};
use lunatic_wasi_api::{build_wasi, LunaticWasi, LunaticWasiCtx};
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Mutex;
use wasmtime::{Linker, ResourceLimiter};
use wasmtime_wasi::p1::WasiP1Ctx;

use crate::DefaultProcessConfig;

#[derive(Debug, Default)]
pub struct DbResources {
    // sqlite data
    sqlite_connections: SQLiteConnections,
    sqlite_statements: SQLiteStatements,
    sqlite_guest_allocator: SQLiteGuestAllocators,
}

pub struct DefaultProcessState {
    // Process id
    pub(crate) id: u64,
    pub(crate) environment: Arc<LunaticEnvironment>,
    pub(crate) distributed: Option<DistributedProcessState>,
    // The WebAssembly runtime
    runtime: Option<WasmtimeRuntime>,
    // The module that this process was spawned from
    module: Option<Arc<WasmtimeCompiledModule<Self>>>,
    // The process configuration
    config: Arc<DefaultProcessConfig>,
    // A space that can be used to temporarily store messages when sending or receiving them.
    // Messages can contain resources that need to be added across multiple host. Likewise,
    // receiving messages is done in two steps, first the message size is returned to allow the
    // guest to reserve enough space, and then it's received. Both of those actions use
    // `message` as a temp space to store messages across host calls.
    message: Option<Message>,
    // Signals sent to the mailbox
    signal_mailbox: (SignalSender, SignalReceiver),
    // Messages sent to the process
    message_mailbox: MessageMailbox,
    // Resources
    resources: Resources,
    // WASI
    wasi: LunaticWasi,
    // WASI stdout stream
    wasi_stdout: Option<StdoutCapture>,
    // WASI stderr stream
    wasi_stderr: Option<StdoutCapture>,
    // Set to true if the WASM module has been instantiated
    initialized: bool,
    // database resources
    db_resources: DbResources,
    registry: Arc<Registry>,
}

impl DefaultProcessState {
    pub fn new(
        environment: Arc<LunaticEnvironment>,
        distributed: Option<DistributedProcessState>,
        runtime: WasmtimeRuntime,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
        registry: Arc<Registry>,
    ) -> Result<Self> {
        let signal_mailbox = unbounded_channel();
        let signal_mailbox = (signal_mailbox.0, Arc::new(Mutex::new(signal_mailbox.1)));
        let message_mailbox = MessageMailbox::default();
        let state = Self {
            id: environment.get_next_process_id(),
            environment,
            distributed,
            runtime: Some(runtime),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry,
            db_resources: DbResources::default(),
        };
        Ok(state)
    }
}

impl ProcessState for DefaultProcessState {
    type Config = DefaultProcessConfig;

    fn new_state(
        &self,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<DefaultProcessConfig>,
    ) -> Result<Self> {
        let signal_mailbox = unbounded_channel();
        let signal_mailbox = (signal_mailbox.0, Arc::new(Mutex::new(signal_mailbox.1)));
        let message_mailbox = MessageMailbox::default();
        let state = Self {
            id: self.environment.get_next_process_id(),
            environment: self.environment.clone(),
            distributed: self.distributed.clone(),
            runtime: self.runtime.clone(),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry: self.registry.clone(),
            db_resources: DbResources::default(),
        };
        Ok(state)
    }

    fn register(linker: &mut Linker<Self>) -> Result<()> {
        lunatic_error_api::register(linker)?;
        lunatic_process_api::register(linker)?;
        lunatic_messaging_api::register(linker)?;
        lunatic_timer_api::register(linker)?;
        lunatic_networking_api::register(linker)?;
        lunatic_version_api::register(linker)?;
        lunatic_wasi_api::register(linker)?;
        lunatic_registry_api::register(linker)?;
        lunatic_distributed_api::register(linker)?;
        lunatic_sqlite_api::register(linker)?;
        #[cfg(feature = "metrics")]
        lunatic_metrics_api::register(linker)?;
        lunatic_trap_api::register(linker)?;
        Ok(())
    }

    fn initialize(&mut self) {
        self.initialized = true;
    }

    fn is_initialized(&self) -> bool {
        self.initialized
    }

    fn runtime(&self) -> &WasmtimeRuntime {
        self.runtime.as_ref().unwrap()
    }

    fn config(&self) -> &Arc<DefaultProcessConfig> {
        &self.config
    }

    fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>> {
        self.module.as_ref().unwrap()
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver) {
        &self.signal_mailbox
    }

    fn message_mailbox(&self) -> &MessageMailbox {
        &self.message_mailbox
    }

    fn config_resources(&self) -> &ConfigResources<<DefaultProcessState as ProcessState>::Config> {
        &self.resources.configs
    }

    fn config_resources_mut(
        &mut self,
    ) -> &mut ConfigResources<<DefaultProcessState as ProcessState>::Config> {
        &mut self.resources.configs
    }

    fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }
}

// The queue for the messages a process sends to other nodes goes away when the process does, once
// the queued messages are sent.
impl Drop for DefaultProcessState {
    fn drop(&mut self) {
        if let Some(distributed) = &self.distributed {
            distributed
                .node_client
                .remove_process_resources(EnvironmentId(self.environment.id()), ProcessId(self.id));
        }
    }
}

impl Debug for DefaultProcessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("process", &self.resources)
            .finish()
    }
}

// Limit the maximum memory of the process depending on the environment it was spawned in.
impl ResourceLimiter for DefaultProcessState {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= self.config().get_max_memory())
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired < 100_000)
    }

    // Allow one instance per store
    fn instances(&self) -> usize {
        1
    }

    // Allow one table per store
    fn tables(&self) -> usize {
        1
    }

    // Allow one memory per store
    fn memories(&self) -> usize {
        1
    }
}

impl ErrorCtx for DefaultProcessState {
    fn error_resources(&self) -> &ErrorResource {
        &self.resources.errors
    }

    fn error_resources_mut(&mut self) -> &mut ErrorResource {
        &mut self.resources.errors
    }
}

impl ProcessCtx<DefaultProcessState> for DefaultProcessState {
    fn mailbox(&mut self) -> &mut MessageMailbox {
        &mut self.message_mailbox
    }

    fn message_scratch_area(&mut self) -> &mut Option<Message> {
        &mut self.message
    }

    fn module_resources(&self) -> &lunatic_process_api::ModuleResources<DefaultProcessState> {
        &self.resources.modules
    }

    fn module_resources_mut(
        &mut self,
    ) -> &mut lunatic_process_api::ModuleResources<DefaultProcessState> {
        &mut self.resources.modules
    }

    fn environment(&self) -> Arc<dyn Environment> {
        self.environment.clone()
    }
}

impl NetworkingCtx for DefaultProcessState {
    fn tcp_listener_resources(&self) -> &lunatic_networking_api::TcpListenerResources {
        &self.resources.tcp_listeners
    }

    fn tcp_listener_resources_mut(&mut self) -> &mut lunatic_networking_api::TcpListenerResources {
        &mut self.resources.tcp_listeners
    }

    fn tcp_stream_resources(&self) -> &lunatic_networking_api::TcpStreamResources {
        &self.resources.tcp_streams
    }

    fn tcp_stream_resources_mut(&mut self) -> &mut lunatic_networking_api::TcpStreamResources {
        &mut self.resources.tcp_streams
    }

    fn tls_listener_resources(&self) -> &lunatic_networking_api::TlsListenerResources {
        &self.resources.tls_listeners
    }

    fn tls_listener_resources_mut(&mut self) -> &mut lunatic_networking_api::TlsListenerResources {
        &mut self.resources.tls_listeners
    }

    fn tls_stream_resources(&self) -> &lunatic_networking_api::TlsStreamResources {
        &self.resources.tls_streams
    }

    fn tls_stream_resources_mut(&mut self) -> &mut lunatic_networking_api::TlsStreamResources {
        &mut self.resources.tls_streams
    }

    fn udp_resources(&self) -> &lunatic_networking_api::UdpResources {
        &self.resources.udp_sockets
    }

    fn udp_resources_mut(&mut self) -> &mut lunatic_networking_api::UdpResources {
        &mut self.resources.udp_sockets
    }

    fn dns_resources(&self) -> &lunatic_networking_api::DnsResources {
        &self.resources.dns_iterators
    }

    fn dns_resources_mut(&mut self) -> &mut lunatic_networking_api::DnsResources {
        &mut self.resources.dns_iterators
    }
}

impl TimerCtx for DefaultProcessState {
    fn timer_resources(&self) -> &TimerResources {
        &self.resources.timers
    }

    fn timer_resources_mut(&mut self) -> &mut TimerResources {
        &mut self.resources.timers
    }
}

impl LunaticWasiCtx for DefaultProcessState {
    fn wasi_mut(&mut self) -> &mut WasiP1Ctx {
        self.wasi.ctx()
    }

    // Redirect the stdout stream
    fn set_stdout(&mut self, stdout: StdoutCapture) {
        self.wasi_stdout = Some(stdout.clone());
        self.wasi.set_stdout(stdout);
    }

    // Redirect the stderr stream
    fn set_stderr(&mut self, stderr: StdoutCapture) {
        self.wasi_stderr = Some(stderr.clone());
        self.wasi.set_stderr(stderr);
    }

    fn get_stdout(&self) -> Option<&StdoutCapture> {
        self.wasi_stdout.as_ref()
    }

    fn get_stderr(&self) -> Option<&StdoutCapture> {
        self.wasi_stderr.as_ref()
    }
}

impl SQLiteCtx for DefaultProcessState {
    fn sqlite_connections(&self) -> &SQLiteConnections {
        &self.db_resources.sqlite_connections
    }

    fn sqlite_connections_mut(&mut self) -> &mut SQLiteConnections {
        &mut self.db_resources.sqlite_connections
    }

    fn sqlite_statements_mut(&mut self) -> &mut SQLiteStatements {
        &mut self.db_resources.sqlite_statements
    }

    fn sqlite_statements(&self) -> &SQLiteStatements {
        &self.db_resources.sqlite_statements
    }

    fn sqlite_guest_allocator(&self) -> &SQLiteGuestAllocators {
        &self.db_resources.sqlite_guest_allocator
    }
    fn sqlite_guest_allocator_mut(&mut self) -> &mut SQLiteGuestAllocators {
        &mut self.db_resources.sqlite_guest_allocator
    }
}

#[derive(Default, Debug)]
pub(crate) struct Resources {
    pub(crate) configs: HashMapId<DefaultProcessConfig>,
    pub(crate) modules: HashMapId<Arc<WasmtimeCompiledModule<DefaultProcessState>>>,
    pub(crate) timers: TimerResources,
    pub(crate) dns_iterators: HashMapId<DnsIterator>,
    pub(crate) tcp_listeners: HashMapId<TcpListener>,
    pub(crate) tcp_streams: HashMapId<Arc<TcpConnection>>,
    pub(crate) tls_listeners: HashMapId<TlsListener>,
    pub(crate) tls_streams: HashMapId<Arc<TlsConnection>>,
    pub(crate) udp_sockets: HashMapId<Arc<UdpSocket>>,
    pub(crate) errors: HashMapId<anyhow::Error>,
}

impl DistributedCtx<LunaticEnvironment> for DefaultProcessState {
    fn distributed_mut(&mut self) -> Result<&mut DistributedProcessState> {
        match self.distributed.as_mut() {
            Some(d) => Ok(d),
            None => Err(anyhow::anyhow!("Distributed is not initialized")),
        }
    }

    fn distributed(&self) -> Result<&DistributedProcessState> {
        match self.distributed.as_ref() {
            Some(d) => Ok(d),
            None => Err(anyhow::anyhow!("Distributed is not initialized")),
        }
    }

    fn module_id(&self) -> u64 {
        self.module
            .as_ref()
            .and_then(|m| m.source().id)
            .unwrap_or(0)
    }

    fn environment_id(&self) -> u64 {
        self.environment.id()
    }

    fn can_spawn(&self) -> bool {
        self.config().can_spawn_processes()
    }

    fn new_dist_state(
        environment: Arc<LunaticEnvironment>,
        distributed: DistributedProcessState,
        runtime: WasmtimeRuntime,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<Self::Config>,
    ) -> Result<Self> {
        let signal_mailbox = unbounded_channel();
        let signal_mailbox = (signal_mailbox.0, Arc::new(Mutex::new(signal_mailbox.1)));
        let message_mailbox = MessageMailbox::default();
        let state = Self {
            id: environment.get_next_process_id(),
            environment,
            distributed: Some(distributed),
            runtime: Some(runtime),
            module: Some(module),
            config: config.clone(),
            message: None,
            signal_mailbox,
            message_mailbox,
            resources: Resources::default(),
            wasi: build_wasi(
                Some(config.command_line_arguments()),
                Some(config.environment_variables()),
                config.preopened_dirs(),
            )?,
            wasi_stdout: None,
            wasi_stderr: None,
            initialized: false,
            registry: Default::default(), // TODO move registry into env?
            db_resources: DbResources::default(),
        };
        Ok(state)
    }
}

mod tests {

    #[tokio::test]
    async fn import_filter_signature_matches() {
        use lunatic_process::registry::Registry;

        use crate::state::DefaultProcessState;
        use crate::DefaultProcessConfig;
        use lunatic_process::env::Environment;
        use lunatic_process::runtimes::wasmtime::WasmtimeRuntime;
        use lunatic_process::wasm::spawn_wasm;
        use std::sync::Arc;

        // The default configuration includes both, the "lunatic::*" and "wasi_*" namespaces.
        let config = DefaultProcessConfig::default();

        // Create wasmtime runtime
        let mut wasmtime_config = wasmtime::Config::new();
        wasmtime_config.consume_fuel(true);
        let runtime = WasmtimeRuntime::new(&wasmtime_config).unwrap();

        let raw_module = wat::parse_file("./wat/all_imports.wat").unwrap();
        let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
        let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));
        let registry = Arc::new(Registry::default());
        let state = DefaultProcessState::new(
            env.clone(),
            None,
            runtime.clone(),
            module.clone(),
            Arc::new(config),
            registry,
        )
        .unwrap();

        env.can_spawn_next_process().await.unwrap();

        spawn_wasm(env, runtime, &module, state, "hello", Vec::new(), None)
            .await
            .unwrap();
    }

    // Spawns the `hello` export of `wat` with the default runtime configuration.
    #[cfg(test)]
    async fn spawn_hello(
        wat: &str,
        config: crate::DefaultProcessConfig,
        stdout: Option<lunatic_stdout_capture::StdoutCapture>,
    ) -> tokio::task::JoinHandle<anyhow::Result<crate::state::DefaultProcessState>> {
        use std::sync::Arc;

        use lunatic_process::env::Environment;
        use lunatic_process::runtimes::wasmtime::{default_config, WasmtimeRuntime};
        use lunatic_process::wasm::spawn_wasm;
        use lunatic_wasi_api::LunaticWasiCtx;

        use crate::state::DefaultProcessState;

        let runtime = WasmtimeRuntime::new(&default_config()).unwrap();
        let raw_module = wat::parse_str(wat).unwrap();
        let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
        let env = Arc::new(lunatic_process::env::LunaticEnvironment::new(0));
        let mut state = DefaultProcessState::new(
            env.clone(),
            None,
            runtime.clone(),
            module.clone(),
            Arc::new(config),
            Default::default(),
        )
        .unwrap();
        if let Some(stdout) = stdout {
            state.set_stdout(stdout);
        }

        env.can_spawn_next_process().await.unwrap();
        let (task, _) = spawn_wasm(env, runtime, &module, state, "hello", Vec::new(), None)
            .await
            .unwrap();
        task
    }

    #[cfg(test)]
    const BUSY_LOOP_WAT: &str = r#"(module (func (export "hello") (loop (br 0))))"#;

    #[tokio::test]
    async fn wasi_stdout_is_captured() {
        use lunatic_stdout_capture::StdoutCapture;

        // Writes "hello" to stdout with `fd_write`.
        let wat = r#"
            (module
                (import "wasi_snapshot_preview1" "fd_write"
                    (func $fd_write (param i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 16) "hello")
                (func (export "hello")
                    (i32.store (i32.const 0) (i32.const 16))
                    (i32.store (i32.const 4) (i32.const 5))
                    (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 8)))))
        "#;
        let stdout = StdoutCapture::new(false);
        let task = spawn_hello(wat, Default::default(), Some(stdout.clone())).await;

        task.await.unwrap().unwrap();
        assert_eq!(stdout.content(), "hello");
    }

    #[tokio::test]
    async fn running_out_of_fuel_traps() {
        use lunatic_process::config::ProcessConfig;

        let mut config = crate::DefaultProcessConfig::default();
        config.set_max_fuel(Some(1));
        let task = spawn_hello(BUSY_LOOP_WAT, config, None).await;

        // Without a fuel limit this would loop forever.
        assert!(task.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn requests_to_missing_processes_report_it() {
        // An untimed request to a process that doesn't exist, trapping unless it returns 1.
        let wat = r#"
            (module
                (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
                (import "lunatic::message" "send_receive_skip_search"
                    (func $send_receive_skip_search (param i64 i64 i64) (result i32)))
                (func (export "hello")
                    (call $create_data (i64.const 0) (i64.const 0))
                    (if (i32.ne (call $send_receive_skip_search
                            (i64.const 999) (i64.const 1) (i64.const -1))
                            (i32.const 1))
                        (then unreachable))))
        "#;
        let task = spawn_hello(wat, Default::default(), None).await;

        tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .expect("the request waited for a reply")
            .unwrap()
            .unwrap();
    }

    #[test]
    fn busy_guest_yields_to_other_tasks() {
        use std::time::Duration;

        let (done_sender, done) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async {
                let _task = spawn_hello(BUSY_LOOP_WAT, Default::default(), None).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
                done_sender.send(()).unwrap();
            });
        });

        // A guest that never yields would starve the single-threaded executor.
        done.recv_timeout(Duration::from_secs(30))
            .expect("the guest didn't yield back to the executor");
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn guest_metrics_are_recorded() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Calls each metrics host function on the metrics "counter", "gauge" and "histo".
        let wat = r#"
            (module
                (import "lunatic::metrics" "counter" (func $counter (param i32 i32 i64)))
                (import "lunatic::metrics" "increment_counter"
                    (func $increment_counter (param i32 i32)))
                (import "lunatic::metrics" "gauge" (func $gauge (param i32 i32 f64)))
                (import "lunatic::metrics" "increment_gauge"
                    (func $increment_gauge (param i32 i32 f64)))
                (import "lunatic::metrics" "decrement_gauge"
                    (func $decrement_gauge (param i32 i32 f64)))
                (import "lunatic::metrics" "histogram" (func $histogram (param i32 i32 f64)))
                (memory (export "memory") 1)
                (data (i32.const 0) "countergaugehisto")
                (func (export "hello")
                    (call $counter (i32.const 0) (i32.const 7) (i64.const 5))
                    (call $increment_counter (i32.const 0) (i32.const 7))
                    (call $gauge (i32.const 7) (i32.const 5) (f64.const 10))
                    (call $increment_gauge (i32.const 7) (i32.const 5) (f64.const 2))
                    (call $decrement_gauge (i32.const 7) (i32.const 5) (f64.const 0.5))
                    (call $histogram (i32.const 12) (i32.const 5) (f64.const 3))))
        "#;
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let task = spawn_hello(wat, Default::default(), None).await;
                    task.await.unwrap().unwrap();
                })
        });

        let metrics: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| (key.key().name().to_owned(), value))
            .collect();
        let value = |name: &str| {
            metrics
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value)
        };
        assert_eq!(value("counter"), Some(&DebugValue::Counter(6)));
        assert_eq!(value("gauge"), Some(&DebugValue::Gauge(11.5.into())));
        assert_eq!(
            value("histo"),
            Some(&DebugValue::Histogram(vec![3.0.into()]))
        );
    }

    // Monitors of processes that don't exist fire right away, after the messages the processes
    // sent.
    #[tokio::test]
    async fn monitors_of_missing_processes_fire() {
        use lunatic_process_api::ProcessConfigCtx;

        let wat = r#"
            (module
                (import "lunatic::process" "monitor" (func $monitor (param i64)))
                (import "lunatic::process" "spawn"
                    (func $spawn (param i64 i64 i64 i32 i32 i32 i32 i32) (result i32)))
                (import "lunatic::process" "process_id" (func $process_id (result i64)))
                (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))
                (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
                (import "lunatic::message" "send" (func $send (param i64) (result i32)))
                (import "lunatic::message" "receive"
                    (func $receive (param i32 i32 i64) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 16) "child")
                ;; 0 for data, 2 for a monitored process' death.
                (func $next (result i32)
                    (call $receive (i32.const 0) (i32.const 0) (i64.const 5000)))
                (func (export "hello")
                    ;; A process that never existed.
                    (call $monitor (i64.const 1000000))
                    (if (i32.ne (call $next) (i32.const 2)) (then unreachable))

                    ;; A process that messaged this one and finished.
                    (i32.store8 (i32.const 128) (i32.const 0x7E))
                    (i64.store (i32.const 129) (call $process_id))
                    (i64.store (i32.const 137) (i64.const 0))
                    (if (call $spawn (i64.const 0) (i64.const -1) (i64.const -1)
                            (i32.const 16) (i32.const 5) (i32.const 128) (i32.const 17)
                            (i32.const 192))
                        (then unreachable))
                    (call $sleep_ms (i64.const 100))
                    (call $monitor (i64.load (i32.const 192)))
                    (if (i32.ne (call $next) (i32.const 0)) (then unreachable))
                    (if (i32.ne (call $next) (i32.const 2)) (then unreachable)))
                (func (export "child") (param $parent i64)
                    (call $create_data (i64.const 0) (i64.const 0))
                    (drop (call $send (local.get $parent)))))
        "#;
        let mut config = crate::DefaultProcessConfig::default();
        config.set_can_spawn_processes(true);
        spawn_hello(wat, config, None).await.await.unwrap().unwrap();
    }

    // A spawn under a name that fails leaves the name free.
    #[tokio::test]
    async fn failed_named_spawns_dont_register_the_name() {
        use lunatic_process_api::ProcessConfigCtx;

        // Spawns itself under "name" with a config whose memory limit is below its memory.
        let wat = r#"
            (module
                (import "lunatic::process" "create_config" (func $create_config (result i64)))
                (import "lunatic::process" "config_set_max_memory"
                    (func $config_set_max_memory (param i64 i64)))
                (import "lunatic::process" "get_or_spawn"
                    (func $get_or_spawn
                        (param i32 i32 i64 i64 i64 i32 i32 i32 i32 i32 i32) (result i32)))
                (memory (export "memory") 1)
                (data (i32.const 16) "name")
                (data (i32.const 24) "hello")
                (func (export "hello")
                    (local $config i64)
                    (local.set $config (call $create_config))
                    (call $config_set_max_memory (local.get $config) (i64.const 1024))
                    (if (i32.ne (i32.const 1)
                            (call $get_or_spawn (i32.const 16) (i32.const 4) (i64.const 0)
                                (local.get $config) (i64.const -1) (i32.const 24) (i32.const 5)
                                (i32.const 0) (i32.const 0) (i32.const 32) (i32.const 40)))
                        (then unreachable))))
        "#;
        let mut config = crate::DefaultProcessConfig::default();
        config.set_can_create_configs(true);
        config.set_can_spawn_processes(true);
        let state = spawn_hello(wat, config, None).await.await.unwrap().unwrap();

        assert_eq!(state.registry.get("name"), None);
        assert!(state.registry.reserve("name").await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn finished_processes_release_their_message_queue() {
        use std::{collections::HashMap, net::TcpListener, sync::Arc};

        use lunatic_distributed::{
            control,
            distributed::{
                self,
                client::{EnvironmentId, NodeId, ProcessId, SendParams},
                server::gen_node_cert,
            },
            quic, DistributedProcessState,
        };
        use lunatic_process::{
            env::{Environment, LunaticEnvironment},
            runtimes::wasmtime::{default_config, WasmtimeRuntime},
        };

        use crate::state::DefaultProcessState;

        // A node registered with a control server.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let control_url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(lunatic_control_axum::server::control_server_from_tcp(
            listener,
        ));
        let http_client = reqwest::Client::new();
        let node_name = uuid::Uuid::new_v4();
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
        let node_address = "127.0.0.1:1".parse().unwrap();
        let control_client = control::Client::new(http_client, reg, node_address, HashMap::new())
            .await
            .unwrap();
        let node_id = control_client.node_id();
        let node_client = distributed::Client::new(node_id, control_client.clone(), quic_client);
        let distributed =
            DistributedProcessState::new(node_id, control_client, node_client.clone())
                .await
                .unwrap();

        let runtime = WasmtimeRuntime::new(&default_config()).unwrap();
        let raw_module = wat::parse_file("./wat/hello.wat").unwrap();
        let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
        let env = Arc::new(LunaticEnvironment::new(1));
        let state = DefaultProcessState::new(
            env.clone(),
            Some(distributed),
            runtime,
            module,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        // The process sends a message to a process on another node.
        let queue = (EnvironmentId(env.id()), ProcessId(state.id));
        node_client
            .send(SendParams {
                env: queue.0,
                src: queue.1,
                node: NodeId(node_id),
                dest: ProcessId(1),
                tag: None,
                data: Vec::new(),
            })
            .await
            .unwrap();
        assert!(node_client.inner.buf_tx.contains_key(&queue));

        // Once its message was sent, the queue goes away.
        drop(state);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while node_client.inner.buf_tx.contains_key(&queue) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the queue wasn't removed");
    }
}
