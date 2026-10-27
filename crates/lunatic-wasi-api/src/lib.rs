use std::sync::{Mutex, PoisonError};

use anyhow::Result;
use lunatic_common_api::{get_memory, IntoTrap, LinkerExt};
use lunatic_process::state::ProcessState;
use lunatic_stdout_capture::StdoutCapture;
use wasmtime::{Caller, Linker};
use wasmtime_wasi::{p1::WasiP1Ctx, FsPerms, WasiCtxBuilder};

/// Create a `LunaticWasi` context from configuration settings.
pub fn build_wasi(
    args: Option<&Vec<String>>,
    envs: Option<&Vec<(String, String)>>,
    dirs: &[(String, String)],
) -> Result<LunaticWasi> {
    let mut wasi = WasiCtxBuilder::new();
    wasi.inherit_stdio();
    if let Some(envs) = envs {
        wasi.envs(envs);
    }
    if let Some(args) = args {
        wasi.args(args);
    }
    for (preopen_dir_path, resolved_path) in dirs {
        wasi.preopened_dir(resolved_path, preopen_dir_path, FsPerms::ReadWrite)?;
    }
    Ok(LunaticWasi(Mutex::new(WasiState::Configuring(Box::new(
        wasi,
    )))))
}

/// The WASI context of a process.
///
/// wasmtime-wasi can't redirect stdio once a context is built, but lunatic redirects a process'
/// stdout and stderr after creating its state (e.g. to capture the output of tests). The context
/// is therefore built lazily on first use, and stdio can be redirected until then.
//
// The process state must be `Sync`, but wasmtime-wasi's contexts aren't. They are only accessed
// through `&mut self`, so the `Mutex` is never locked (see `Mutex::get_mut`); it only makes the
// type `Sync`.
pub struct LunaticWasi(Mutex<WasiState>);

enum WasiState {
    Configuring(Box<WasiCtxBuilder>),
    Built(Box<WasiP1Ctx>),
}

impl LunaticWasi {
    /// Redirect the stdout stream. Has no effect once the process has used WASI.
    pub fn set_stdout(&mut self, stdout: StdoutCapture) {
        match self.state() {
            WasiState::Configuring(builder) => {
                builder.stdout(stdout);
            }
            WasiState::Built(_) => log::error!("stdout can't be redirected after WASI was used"),
        }
    }

    /// Redirect the stderr stream. Has no effect once the process has used WASI.
    pub fn set_stderr(&mut self, stderr: StdoutCapture) {
        match self.state() {
            WasiState::Configuring(builder) => {
                builder.stderr(stderr);
            }
            WasiState::Built(_) => log::error!("stderr can't be redirected after WASI was used"),
        }
    }

    /// Returns the WASI context, building it on first use.
    pub fn ctx(&mut self) -> &mut WasiP1Ctx {
        let state = self.state();
        if let WasiState::Configuring(builder) = state {
            *state = WasiState::Built(Box::new(builder.build_p1()));
        }
        match state {
            WasiState::Built(ctx) => ctx,
            WasiState::Configuring(_) => unreachable!("the context was built above"),
        }
    }

    fn state(&mut self) -> &mut WasiState {
        self.0.get_mut().unwrap_or_else(PoisonError::into_inner)
    }
}

pub trait LunaticWasiConfigCtx {
    fn add_environment_variable(&mut self, key: String, value: String);
    fn add_command_line_argument(&mut self, argument: String);
    fn preopen_dir(&mut self, dir: String);
}

pub trait LunaticWasiCtx {
    fn wasi_mut(&mut self) -> &mut WasiP1Ctx;
    fn set_stdout(&mut self, stdout: StdoutCapture);
    fn get_stdout(&self) -> Option<&StdoutCapture>;
    fn set_stderr(&mut self, stderr: StdoutCapture);
    fn get_stderr(&self) -> Option<&StdoutCapture>;
}

// Register WASI APIs to the linker
pub fn register<T>(linker: &mut Linker<T>) -> Result<()>
where
    T: ProcessState + LunaticWasiCtx + Send + 'static,
    T::Config: LunaticWasiConfigCtx,
{
    // Register all wasi host functions
    wasmtime_wasi::p1::add_to_linker_async(linker, |ctx| ctx.wasi_mut())?;

    // Register host functions to configure wasi
    linker.func_wrap_anyhow(
        "lunatic::wasi",
        "config_add_environment_variable",
        add_environment_variable,
    )?;
    linker.func_wrap_anyhow(
        "lunatic::wasi",
        "config_add_command_line_argument",
        add_command_line_argument,
    )?;
    linker.func_wrap_anyhow("lunatic::wasi", "config_preopen_dir", preopen_dir)?;

    Ok(())
}

// Adds environment variable to a configuration.
//
// Traps:
// * If the config ID doesn't exist.
// * If the key or value string is not a valid utf8 string.
// * If any of the memory slices falls outside the memory.
fn add_environment_variable<T>(
    mut caller: Caller<T>,
    config_id: u64,
    key_ptr: u32,
    key_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let key_str = memory
        .data(&caller)
        .get(key_ptr as usize..(key_ptr + key_len) as usize)
        .or_trap("lunatic::wasi::config_add_environment_variable")?;
    let key = std::str::from_utf8(key_str)
        .or_trap("lunatic::wasi::config_add_environment_variable")?
        .to_string();
    let value_str = memory
        .data(&caller)
        .get(value_ptr as usize..(value_ptr + value_len) as usize)
        .or_trap("lunatic::wasi::config_add_environment_variable")?;
    let value = std::str::from_utf8(value_str)
        .or_trap("lunatic::wasi::config_add_environment_variable")?
        .to_string();

    caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::config_set_max_memory: Config ID doesn't exist")?
        .add_environment_variable(key, value);
    Ok(())
}

// Adds command line argument to a configuration.
//
// Traps:
// * If the config ID doesn't exist.
// * If the argument string is not a valid utf8 string.
// * If any of the memory slices falls outside the memory.
fn add_command_line_argument<T>(
    mut caller: Caller<T>,
    config_id: u64,
    argument_ptr: u32,
    argument_len: u32,
) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let argument_str = memory
        .data(&caller)
        .get(argument_ptr as usize..(argument_ptr + argument_len) as usize)
        .or_trap("lunatic::wasi::add_command_line_argument")?;
    let argument = std::str::from_utf8(argument_str)
        .or_trap("lunatic::wasi::add_command_line_argument")?
        .to_string();

    caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::add_command_line_argument: Config ID doesn't exist")?
        .add_command_line_argument(argument);
    Ok(())
}

// Mark a directory as preopened in the configuration.
//
// Traps:
// * If the config ID doesn't exist.
// * If the directory string is not a valid utf8 string.
// * If any of the memory slices falls outside the memory.
fn preopen_dir<T>(mut caller: Caller<T>, config_id: u64, dir_ptr: u32, dir_len: u32) -> Result<()>
where
    T: ProcessState,
    T::Config: LunaticWasiConfigCtx,
{
    let memory = get_memory(&mut caller)?;
    let dir_str = memory
        .data(&caller)
        .get(dir_ptr as usize..(dir_ptr + dir_len) as usize)
        .or_trap("lunatic::wasi::preopen_dir")?;
    let dir = std::str::from_utf8(dir_str)
        .or_trap("lunatic::wasi::preopen_dir")?
        .to_string();

    caller
        .data_mut()
        .config_resources_mut()
        .get_mut(config_id)
        .or_trap("lunatic::wasi::preopen_dir: Config ID doesn't exist")?
        .preopen_dir(dir);
    Ok(())
}
