//! A control and two nodes, where a guest on one node works with processes it spawns on the other
//! node. Fails on a timeout instead of hanging.

use std::{
    io::{BufRead, BufReader, Read},
    net::UdpSocket,
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use tempfile::TempDir;

const TIMEOUT: Duration = Duration::from_secs(60);

// Output lines that mean the scenario failed, on any of the processes.
const FAILURES: [&str; 7] = [
    "distributed failed",
    "dead process failed",
    "monitor failed",
    "named spawn failed",
    "node down failed",
    "failed, notifying",
    "Error running wasm",
];

type Line = (&'static str, String);

// A running `lunatic` process, killed when dropped.
struct Lunatic(Child);

impl Lunatic {
    fn start(dir: &Path, name: &'static str, args: &[&str], lines: &Sender<Line>) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_lunatic"))
            .args(args)
            .current_dir(dir)
            .env("RUST_LOG", "info")
            // Otherwise `lunatic` may think it's running as a `cargo test` runner.
            .env_remove("CARGO_MANIFEST_DIR")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        forward_lines(child.stdout.take().unwrap(), name, lines.clone());
        forward_lines(child.stderr.take().unwrap(), name, lines.clone());
        Self(child)
    }
}

impl Drop for Lunatic {
    fn drop(&mut self) {
        self.0.kill().ok();
        self.0.wait().ok();
    }
}

fn forward_lines(output: impl Read + Send + 'static, name: &'static str, lines: Sender<Line>) {
    thread::spawn(move || {
        for line in BufReader::new(output).lines().map_while(Result::ok) {
            if lines.send((name, line)).is_err() {
                break;
            }
        }
    });
}

// The output of all `lunatic` processes of a test.
struct Output {
    lines: Receiver<Line>,
    seen: Vec<Line>,
    deadline: Instant,
}

impl Output {
    // Waits for an output line of `name` that contains `pattern`, and returns it. The line may
    // have been printed while waiting for another one.
    fn wait_for(&mut self, name: &str, pattern: &str) -> String {
        let printed = |(line_name, line): &Line| *line_name == name && line.contains(pattern);
        if let Some((_, line)) = self.seen.iter().find(|line| printed(line)) {
            return line.clone();
        }
        loop {
            let timeout = self.deadline.saturating_duration_since(Instant::now());
            let Ok(line) = self.lines.recv_timeout(timeout) else {
                panic!(
                    "Timed out waiting for {} to print {:?}:\n{}",
                    name,
                    pattern,
                    self.dump()
                );
            };
            self.seen.push(line.clone());
            if FAILURES.iter().any(|failure| line.1.contains(failure)) {
                panic!("{} failed:\n{}", line.0, self.dump());
            }
            if printed(&line) {
                return line.1;
            }
        }
    }

    fn dump(&self) -> String {
        self.seen
            .iter()
            .map(|(name, line)| format!("[{name}] {line}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn free_udp_port() -> (UdpSocket, u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    (socket, port)
}

// A control, node A, and node B running a guest, which works with processes on node A.
struct Cluster {
    output: Output,
    // Dropped before the directory they run in.
    _control: Lunatic,
    node_a: Lunatic,
    _node_b: Lunatic,
    _dir: TempDir,
}

impl Cluster {
    fn start(wat: &str) -> Self {
        let dir = TempDir::new().unwrap();
        let guest = wat::parse_file(Path::new(env!("CARGO_MANIFEST_DIR")).join(wat)).unwrap();
        std::fs::write(dir.path().join("guest.wasm"), guest).unwrap();

        let (sender, lines) = mpsc::channel();
        let mut output = Output {
            lines,
            seen: Vec::new(),
            deadline: Instant::now() + TIMEOUT,
        };

        let control = Lunatic::start(dir.path(), "control", &["control"], &sender);
        let control_url = output.wait_for("control", "Register URL: ");
        let control_url = control_url
            .split("Register URL: ")
            .nth(1)
            .unwrap()
            .to_owned();

        // Hold the ports until both are picked, so that the nodes don't get the same one.
        let (socket_a, port_a) = free_udp_port();
        let (socket_b, port_b) = free_udp_port();
        drop((socket_a, socket_b));

        let node_a = format!("127.0.0.1:{port_a}");
        let node_a = Lunatic::start(
            dir.path(),
            "node A",
            &["node", "--bind-socket", &node_a, &control_url],
            &sender,
        );
        output.wait_for("node A", "Registration successful");

        let node_b = format!("127.0.0.1:{port_b}");
        let node_b = Lunatic::start(
            dir.path(),
            "node B",
            &[
                "node",
                "--bind-socket",
                &node_b,
                "--wasm",
                "guest.wasm",
                &control_url,
            ],
            &sender,
        );
        Self {
            output,
            _control: control,
            node_a,
            _node_b: node_b,
            _dir: dir,
        }
    }
}

// Runs the guest `wat` on node B, until it prints `success`.
fn run_guest(wat: &str, success: &str) {
    Cluster::start(wat).output.wait_for("node B", success);
}

// Spawns processes on the other node and exchanges messages with them, after a spawn of a missing
// module failed (`wat/distributed.wat`).
#[test]
fn nodes_spawn_processes_on_each_other() {
    run_guest("wat/distributed.wat", "distributed ok");
}

// Requests to and messages for a process that exited on the other node
// (`wat/dead_process.wat`).
#[test]
fn messages_to_dead_processes_are_dropped() {
    run_guest("wat/dead_process.wat", "dead process ok");
}

// Monitors of processes on another node fire after the messages the processes sent, and for
// processes that finished or never existed (`wat/monitor.wat`).
#[test]
fn remote_monitors_fire_after_the_last_message() {
    run_guest("wat/monitor.wat", "monitor ok");
}

// A node that crashes leaves the cluster within its lease, and monitors of its processes fire
// (`wat/node_down.wat`).
#[test]
fn crashed_nodes_leave_the_cluster() {
    let mut cluster = Cluster::start("wat/node_down.wat");
    cluster.output.wait_for("node B", "monitoring");

    // `Child::kill` sends SIGKILL, so node A can't tell the control it's leaving.
    cluster.node_a.0.kill().unwrap();
    let killed = Instant::now();
    cluster.output.wait_for("node B", "left the cluster");
    // The 5s lease, plus up to a second for node B to refresh its node list.
    let left_after = killed.elapsed();
    assert!(
        left_after < Duration::from_secs(8),
        "node A left the cluster {:?} after it was killed",
        left_after
    );
    cluster.output.wait_for("node B", "node down ok");
}

// Processes concurrently spawning a process under the same name on another node get the same
// process (`wat/named_spawn.wat`).
#[test]
fn concurrent_named_spawns_spawn_one_process() {
    run_guest("wat/named_spawn.wat", "named spawn ok");
}

// A node that was unresponsive for longer than its lease is dropped from the cluster for good:
// once it resumes, the control rejects it, and it exits with an error. Unix only: the node is made
// unresponsive with SIGSTOP.
#[cfg(unix)]
#[test]
fn nodes_dropped_from_the_cluster_exit() {
    let mut cluster = Cluster::start("wat/node_down.wat");
    cluster.output.wait_for("node B", "monitoring");

    signal(&cluster.node_a, "-STOP");
    cluster.output.wait_for("node B", "left the cluster");
    signal(&cluster.node_a, "-CONT");

    cluster
        .output
        .wait_for("node A", "dropped this node from the cluster");
    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = cluster.node_a.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "node A didn't exit");
        thread::sleep(Duration::from_millis(50));
    };
    assert!(!status.success(), "node A exited with {}", status);
}

#[cfg(unix)]
fn signal(lunatic: &Lunatic, signal: &str) {
    let status = Command::new("kill")
        .args([signal, &lunatic.0.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
}
