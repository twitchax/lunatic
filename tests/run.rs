//! `lunatic run` fails when the main process fails, so callers like test runners notice.

use std::{
    path::PathBuf,
    process::{Command, ExitStatus},
};

// Runs a module whose `_start` function has the given body.
fn run(name: &str, start_body: &str) -> ExitStatus {
    let wat = format!(
        r#"(module
            (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
            (memory (export "memory") 1)
            (func (export "_start") {start_body}))"#
    );
    let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}.wat"));
    std::fs::write(&path, wat).unwrap();
    Command::new(env!("CARGO_BIN_EXE_lunatic"))
        .arg("run")
        .arg(&path)
        // Otherwise `lunatic` may think it's running as a `cargo test` runner.
        .env_remove("CARGO_MANIFEST_DIR")
        .output()
        .unwrap()
        .status
}

#[test]
fn successful_main_process_exits_with_success() {
    assert!(run("returns", "nop").success());
    assert!(run("exits_with_zero", "(call $proc_exit (i32.const 0))").success());
}

#[test]
fn failed_main_process_exits_with_failure() {
    assert!(!run("traps", "unreachable").success());
    assert!(!run("exits_with_three", "(call $proc_exit (i32.const 3))").success());
}
