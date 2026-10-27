use std::sync::Arc;

use anyhow::Result;
use wasmtime::ResourceLimiter;

use crate::{
    config::{ProcessConfig, UNIT_OF_COMPUTE_IN_INSTRUCTIONS},
    state::ProcessState,
    ExecutionResult, ResultValue,
};

use super::RawWasm;

#[derive(Clone)]
pub struct WasmtimeRuntime {
    engine: wasmtime::Engine,
}

impl WasmtimeRuntime {
    pub fn new(config: &wasmtime::Config) -> Result<Self> {
        let engine = wasmtime::Engine::new(config)?;
        Ok(Self { engine })
    }

    /// Compiles a wasm module to machine code and performs type-checking on host functions.
    pub fn compile_module<T>(&self, data: RawWasm) -> Result<WasmtimeCompiledModule<T>>
    where
        T: ProcessState,
    {
        let module = wasmtime::Module::new(&self.engine, data.as_slice())?;
        let mut linker = wasmtime::Linker::new(&self.engine);
        // Register host functions to linker.
        <T as ProcessState>::register(&mut linker)?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let compiled_module = WasmtimeCompiledModule::new(data, module, instance_pre);
        Ok(compiled_module)
    }

    pub async fn instantiate<T>(
        &self,
        compiled_module: &WasmtimeCompiledModule<T>,
        state: T,
    ) -> Result<WasmtimeInstance<T>>
    where
        T: ProcessState + Send + ResourceLimiter,
    {
        let max_fuel = state.config().get_max_fuel();
        let mut store = wasmtime::Store::new(&self.engine, state);
        // Set limits of the store
        store.limiter(|state| state);
        // Define maximum fuel (in units of compute) and yield after each unit. Wasmtime traps once
        // the fuel runs out. If no limit is specified use maximum.
        let max_fuel = max_fuel.map_or(u64::MAX, |max_fuel| {
            max_fuel.saturating_mul(UNIT_OF_COMPUTE_IN_INSTRUCTIONS)
        });
        store.set_fuel(max_fuel)?;
        store.fuel_async_yield_interval(Some(UNIT_OF_COMPUTE_IN_INSTRUCTIONS))?;
        // Create instance
        let instance = compiled_module
            .instantiator()
            .instantiate_async(&mut store)
            .await?;
        // Mark state as initialized
        store.data_mut().initialize();
        Ok(WasmtimeInstance { store, instance })
    }
}

pub struct WasmtimeCompiledModule<T> {
    inner: Arc<WasmtimeCompiledModuleInner<T>>,
}

pub struct WasmtimeCompiledModuleInner<T> {
    source: RawWasm,
    module: wasmtime::Module,
    instance_pre: wasmtime::InstancePre<T>,
}

impl<T> WasmtimeCompiledModule<T> {
    pub fn new(
        source: RawWasm,
        module: wasmtime::Module,
        instance_pre: wasmtime::InstancePre<T>,
    ) -> WasmtimeCompiledModule<T> {
        let inner = Arc::new(WasmtimeCompiledModuleInner {
            source,
            module,
            instance_pre,
        });
        Self { inner }
    }

    pub fn exports(&self) -> impl ExactSizeIterator<Item = wasmtime::ExportType<'_>> {
        self.inner.module.exports()
    }

    pub fn source(&self) -> &RawWasm {
        &self.inner.source
    }

    pub fn instantiator(&self) -> &wasmtime::InstancePre<T> {
        &self.inner.instance_pre
    }
}

impl<T> Clone for WasmtimeCompiledModule<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

pub struct WasmtimeInstance<T>
where
    T: Send + 'static,
{
    store: wasmtime::Store<T>,
    instance: wasmtime::Instance,
}

impl<T> WasmtimeInstance<T>
where
    T: Send + 'static,
{
    pub async fn call(mut self, function: &str, params: Vec<wasmtime::Val>) -> ExecutionResult<T> {
        let entry = self.instance.get_func(&mut self.store, function);

        if entry.is_none() {
            return ExecutionResult {
                state: self.store.into_data(),
                result: ResultValue::SpawnError(format!("Function '{function}' not found")),
            };
        }

        let result = entry
            .unwrap()
            .call_async(&mut self.store, &params, &mut [])
            .await;

        ExecutionResult {
            state: self.store.into_data(),
            result: match result {
                Ok(()) => ResultValue::Ok,
                Err(err) => {
                    // If the trap is a result of calling `proc_exit(0)`, treat it as an no-error finish.
                    match err.downcast_ref::<wasmtime_wasi::I32Exit>() {
                        Some(wasmtime_wasi::I32Exit(0)) => ResultValue::Ok,
                        _ => ResultValue::Failed(err.to_string()),
                    }
                }
            },
        }
    }
}

pub fn default_config() -> wasmtime::Config {
    let mut config = wasmtime::Config::new();
    config
        .debug_info(false)
        // The behavior of fuel running out is defined on the Store
        .consume_fuel(true)
        .wasm_reference_types(true)
        .wasm_bulk_memory(true)
        .wasm_multi_value(true)
        .wasm_multi_memory(true)
        .cranelift_opt_level(wasmtime::OptLevel::SpeedAndSize)
        // Allocate resources on demand because we can't predict how many process will exist
        .allocation_strategy(wasmtime::InstanceAllocationStrategy::OnDemand)
        // Always use static memories: reserve the full address space up front and never move them
        .memory_may_move(false);
    config
}
