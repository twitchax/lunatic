//! The guest SDK (lunatic-rs) is compiled against these host functions, so their names and
//! signatures must not change by accident (e.g. through a wasmtime or wasmtime-wasi upgrade).

use std::sync::Arc;

use lunatic_process::{
    env::LunaticEnvironment,
    registry::Registry,
    runtimes::wasmtime::{default_config, WasmtimeRuntime},
    state::ProcessState,
};
use lunatic_runtime::{DefaultProcessConfig, DefaultProcessState};
use wasmtime::{Engine, Extern, Linker, Store};

#[tokio::test]
async fn host_functions_match_snapshot() {
    let config = default_config();
    let engine = Engine::new(&config).unwrap();
    let runtime = WasmtimeRuntime::new(&config).unwrap();
    let raw_module = wat::parse_file("./wat/hello.wat").unwrap();
    let module = Arc::new(runtime.compile_module(raw_module.into()).unwrap());
    let state = DefaultProcessState::new(
        Arc::new(LunaticEnvironment::new(0)),
        None,
        runtime,
        module,
        Arc::new(DefaultProcessConfig::default()),
        Arc::new(Registry::default()),
    )
    .unwrap();

    let mut linker = Linker::new(&engine);
    <DefaultProcessState as ProcessState>::register(&mut linker).unwrap();
    let mut store = Store::new(&engine, state);
    let definitions: Vec<_> = linker
        .iter(&mut store)
        .map(|(module, name, definition)| (module.to_owned(), name.to_owned(), definition))
        .collect();

    let mut functions: Vec<String> = definitions
        .into_iter()
        .map(|(module, name, definition)| {
            let Extern::Func(func) = definition else {
                panic!("{}::{} is not a function", module, name);
            };
            let ty = func.ty(&store);
            let params: Vec<String> = ty.params().map(|param| param.to_string()).collect();
            let results: Vec<String> = ty.results().map(|result| result.to_string()).collect();
            format!(
                "{module} {name} ({}) -> ({})",
                params.join(", "),
                results.join(", ")
            )
        })
        .collect();
    functions.sort();

    let expected: Vec<&str> = include_str!("host_abi.txt").lines().collect();
    assert_eq!(functions, expected);
}
