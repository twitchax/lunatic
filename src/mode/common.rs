use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use clap::Args;

use lunatic_distributed::DistributedProcessState;
use lunatic_process::{
    env::{Environment, LunaticEnvironment, LunaticEnvironments},
    runtimes::{wasmtime::WasmtimeRuntime, RawWasm},
    wasm::spawn_wasm,
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_runtime::{DefaultProcessConfig, DefaultProcessState};

#[derive(Args, Debug)]
pub struct WasmArgs {}

pub struct RunWasm {
    pub path: PathBuf,
    pub wasm_args: Vec<String>,
    pub dir: Vec<PathBuf>,

    pub runtime: WasmtimeRuntime,
    // Never read, but keeps the environments alive for the duration of the run.
    #[allow(dead_code)]
    pub envs: Arc<LunaticEnvironments>,
    pub env: Arc<LunaticEnvironment>,
    pub distributed: Option<DistributedProcessState>,
}

/// Initializes the logger, using `default_filter` unless `RUST_LOG` is set.
pub fn init_logger(default_filter: &str) {
    logger(env_logger::Env::default().default_filter_or(default_filter)).init();
}

fn logger(env: env_logger::Env) -> env_logger::Builder {
    let mut builder = env_logger::Builder::new();
    builder
        // wasmtime-wasi traces every WASI call at the info level. Keep these quiet unless they are
        // asked for explicitly in `RUST_LOG`.
        .filter_module("wasmtime_wasi", log::LevelFilter::Warn)
        .filter_module("tracing::span", log::LevelFilter::Warn)
        .parse_env(env);
    builder
}

pub async fn run_wasm(args: RunWasm) -> Result<()> {
    let mut config = DefaultProcessConfig::default();
    // Allow initial process to compile modules, create configurations and spawn sub-processes
    config.set_can_compile_modules(true);
    config.set_can_create_configs(true);
    config.set_can_spawn_processes(true);

    // Path to wasm file
    let path = args.path;

    // Set correct command line arguments for the guest
    let filename = path.file_name().unwrap().to_string_lossy().to_string();
    let mut wasi_args = vec![filename];
    wasi_args.extend(args.wasm_args);
    config.set_command_line_arguments(wasi_args);

    // Inherit environment variables
    config.set_environment_variables(std::env::vars().collect());

    // Always preopen the current dir
    config.preopen_dir(".");
    for dir in args.dir {
        if let Some(s) = dir.as_os_str().to_str() {
            config.preopen_dir(s);
        }
    }

    // Spawn main process
    let module = std::fs::read(&path).map_err(|err| match err.kind() {
        std::io::ErrorKind::NotFound => anyhow!("Module '{}' not found", path.display()),
        _ => err.into(),
    })?;
    let module: RawWasm = if let Some(dist) = args.distributed.as_ref() {
        dist.control.add_module(module).await?
    } else {
        module.into()
    };

    let module = Arc::new(args.runtime.compile_module::<DefaultProcessState>(module)?);
    let state = DefaultProcessState::new(
        args.env.clone(),
        args.distributed,
        args.runtime.clone(),
        module.clone(),
        Arc::new(config),
        Default::default(),
    )
    .unwrap();

    args.env.can_spawn_next_process().await?;
    let (task, _) = spawn_wasm(
        args.env,
        args.runtime,
        &module,
        state,
        "_start",
        Vec::new(),
        None,
    )
    .await
    .context(format!(
        "Failed to spawn process from {}::_start()",
        path.to_string_lossy()
    ))?;

    // Wait on the main process to finish. Its failure was already logged, but must still make the
    // command fail.
    task.await
        .map_err(|e| anyhow!(e.to_string()))?
        .map(|_| ())
        .map_err(|_| anyhow!("Process {}::_start() failed", path.to_string_lossy()))
}

#[cfg(feature = "prometheus")]
#[derive(Args, Debug)]
pub struct PrometheusArgs {
    /// Enables the prometheus metrics exporter
    #[arg(long)]
    pub prometheus: bool,

    /// Address to bind the prometheus http listener to
    #[arg(long, value_name = "PROMETHEUS_HTTP_ADDRESS", requires = "prometheus")]
    pub prometheus_http: Option<std::net::SocketAddr>,
}

#[cfg(feature = "prometheus")]
pub fn prometheus(http_socket: Option<std::net::SocketAddr>, node_id: Option<u64>) -> Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(http_socket.unwrap_or_else(|| "0.0.0.0:9927".parse().unwrap()))
        .add_global_label("node_id", node_id.unwrap_or(0).to_string())
        .install()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use log::{Level, Log, Metadata};

    // Whether a logger configured with `filter` logs info messages of `target`.
    fn logs_info(filter: &str, target: &str) -> bool {
        let env = env_logger::Env::new()
            .filter("LUNATIC_UNSET_LOG_FILTER")
            .default_filter_or(filter);
        let logger = super::logger(env).build();
        logger.enabled(
            &Metadata::builder()
                .level(Level::Info)
                .target(target)
                .build(),
        )
    }

    #[test]
    fn wasi_call_traces_are_only_logged_on_request() {
        assert!(logs_info("info", "lunatic_process"));
        assert!(!logs_info("info", "wasmtime_wasi::p1"));
        assert!(logs_info("info,wasmtime_wasi=info", "wasmtime_wasi::p1"));
    }
}
