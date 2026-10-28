use std::{
    fs::{create_dir_all, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use anyhow::{anyhow, Result};
use toml::{value::Table, Value};

/// The compilation target `lunatic init` configures (`wasm32-wasi` before Rust 1.84).
pub(crate) const TARGET: &str = "wasm32-wasip1";

pub(crate) fn start() -> Result<()> {
    // Check if the current directory is a Rust cargo project.
    if !Path::new("Cargo.toml").exists() {
        return Err(anyhow!("Must be called inside a cargo project"));
    }

    // Open or create cargo config file.
    create_dir_all(".cargo").unwrap();
    let mut config_toml = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(".cargo/config.toml")
        .unwrap();

    let mut content = String::new();
    config_toml.read_to_string(&mut content).unwrap();

    let mut content = content.parse::<Table>().unwrap();
    let table = &mut content;

    // Set correct target
    match table.get_mut("build") {
        Some(value) => {
            let build = value
                .as_table_mut()
                .expect("wrong `.cargo/config.toml` format");
            match build.get_mut("target") {
                Some(target)
                    if target.as_str().expect("wrong `.cargo/config.toml` format") != TARGET =>
                {
                    return Err(
                        anyhow!("value `build.target` inside `.cargo/config.toml` already set to something else than `{TARGET}`")
                    );
                }
                None => {
                    // If value is missing, add it.
                    build.insert("target".to_owned(), Value::String(TARGET.to_owned()));
                }
                _ => {
                    // If correct value is set don't do anything.
                }
            }
        }
        None => {
            let mut new_build = Table::new();
            new_build.insert("target".to_owned(), Value::String(TARGET.to_owned()));
            table.insert("build".to_owned(), Value::Table(new_build));
        }
    };

    // Set correct runner
    match table.get_mut("target") {
        Some(value) => {
            let target = value
                .as_table_mut()
                .expect("wrong `.cargo/config.toml` format");
            match target.get_mut(TARGET) {
                Some(value) => {
                    let target = value
                        .as_table_mut()
                        .expect("wrong `.cargo/config.toml` format");
                    match target.get_mut("runner") {
                        Some(runner)
                            if runner.as_str().expect("wrong `.cargo/config.toml` format")
                                == "lunatic" =>
                        {
                            // Update old runner to new one
                            target.insert(
                                "runner".to_owned(),
                                Value::String("lunatic run".to_owned()),
                            );
                        }
                        Some(runner)
                            if runner.as_str().expect("wrong `.cargo/config.toml` format")
                                != "lunatic run" =>
                        {
                            return Err(
                            anyhow!("value `target.{TARGET}.runner` inside `.cargo/config.toml` already set to something else than `lunatic run`")
                        );
                        }
                        None => {
                            // If value is missing, add it.
                            target.insert(
                                "runner".to_owned(),
                                Value::String("lunatic run".to_owned()),
                            );
                        }
                        _ => {
                            // If correct value is set don't do anything.
                        }
                    }
                }
                None => {
                    // Create sub-table for the target with runner set.
                    let mut new_wasm32_wasi = Table::new();
                    new_wasm32_wasi
                        .insert("runner".to_owned(), Value::String("lunatic run".to_owned()));
                    target.insert(TARGET.to_owned(), Value::Table(new_wasm32_wasi));
                }
            }
        }
        None => {
            // Create sub-table for the target with runner set.
            let mut new_wasm32_wasi = Table::new();
            new_wasm32_wasi.insert("runner".to_owned(), Value::String("lunatic run".to_owned()));
            // Create table `target` with a sub-table for the target.
            let mut new_target = Table::new();
            new_target.insert(TARGET.to_owned(), Value::Table(new_wasm32_wasi));
            table.insert("target".to_owned(), Value::Table(new_target));
        }
    };

    let new_config = toml::to_string(table).unwrap();
    // Truncate existing config
    config_toml.set_len(0).unwrap();
    config_toml.rewind().unwrap();
    config_toml
        .write_all(new_config.as_bytes())
        .expect("unable to write new config to `.cargo/config.toml`");

    println!("Cargo project initialized!");

    Ok(())
}
