//! `lunatic init` and `cargo lunatic` configure cargo for the `wasm32-wasip1` target.

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

use tempfile::TempDir;

fn init(project: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_lunatic"))
        .arg("init")
        .current_dir(project)
        // Otherwise `lunatic` may think it's running as a `cargo test` runner.
        .env_remove("CARGO_MANIFEST_DIR")
        .output()
        .unwrap()
}

fn cargo_project(cargo_config: Option<&str>) -> TempDir {
    let project = TempDir::new().unwrap();
    fs::write(
        project.path().join("Cargo.toml"),
        "[package]\nname = \"app\"\n",
    )
    .unwrap();
    if let Some(cargo_config) = cargo_config {
        fs::create_dir(project.path().join(".cargo")).unwrap();
        fs::write(project.path().join(".cargo/config.toml"), cargo_config).unwrap();
    }
    project
}

fn cargo_config(project: &Path) -> toml::Table {
    fs::read_to_string(project.join(".cargo/config.toml"))
        .unwrap()
        .parse()
        .unwrap()
}

#[test]
fn init_configures_the_target_and_its_runner() {
    let project = cargo_project(Some("[alias]\nb = \"build\"\n"));
    assert!(init(project.path()).status.success());
    // Running it again keeps the configuration.
    assert!(init(project.path()).status.success());

    let config = cargo_config(project.path());
    assert_eq!(config["build"]["target"].as_str(), Some("wasm32-wasip1"));
    assert_eq!(
        config["target"]["wasm32-wasip1"]["runner"].as_str(),
        Some("lunatic run")
    );
    assert_eq!(config["alias"]["b"].as_str(), Some("build"));
}

#[test]
fn init_keeps_a_different_target() {
    let project = cargo_project(Some("[build]\ntarget = \"wasm32-unknown-unknown\"\n"));
    let output = init(project.path());
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("wasm32-wasip1"));

    let config = cargo_config(project.path());
    assert_eq!(
        config["build"]["target"].as_str(),
        Some("wasm32-unknown-unknown")
    );
}

#[cfg(unix)]
#[test]
fn cargo_lunatic_builds_for_the_target() {
    use std::os::unix::fs::PermissionsExt;

    // A stand-in for cargo that prints the target configuration it was given.
    let bin = TempDir::new().unwrap();
    let cargo = bin.path().join("cargo");
    fs::write(
        &cargo,
        "#!/bin/sh\necho \"$CARGO_BUILD_TARGET $CARGO_TARGET_WASM32_WASIP1_RUNNER $*\"\n",
    )
    .unwrap();
    fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755)).unwrap();

    let path = format!(
        "{}:{}",
        bin.path().display(),
        std::env::var("PATH").unwrap()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_cargo-lunatic"))
        .args(["lunatic", "build", "--release"])
        .env("PATH", path)
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "wasm32-wasip1 lunatic build --release\n"
    );
}
