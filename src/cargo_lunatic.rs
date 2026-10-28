use std::{
    env::{args_os, set_var},
    process::{exit, Command},
};

fn main() {
    set_var("CARGO_BUILD_TARGET", "wasm32-wasip1");
    set_var("CARGO_TARGET_WASM32_WASIP1_RUNNER", "lunatic");
    exit(
        Command::new("cargo")
            .args(args_os().skip(2))
            .status()
            .unwrap()
            .code()
            .unwrap(),
    );
}
