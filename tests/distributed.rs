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
const FAILURES: [&str; 4] = [
    "distributed failed",
    "dead process failed",
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
    seen: Vec<String>,
    deadline: Instant,
}

impl Output {
    // Waits for an output line of `name` that contains `pattern`, and returns it.
    fn wait_for(&mut self, name: &str, pattern: &str) -> String {
        loop {
            let timeout = self.deadline.saturating_duration_since(Instant::now());
            let Ok((line_name, line)) = self.lines.recv_timeout(timeout) else {
                panic!(
                    "Timed out waiting for {} to print {:?}:\n{}",
                    name,
                    pattern,
                    self.seen.join("\n")
                );
            };
            self.seen.push(format!("[{line_name}] {line}"));
            if FAILURES.iter().any(|failure| line.contains(failure)) {
                panic!("{} failed:\n{}", line_name, self.seen.join("\n"));
            }
            if line_name == name && line.contains(pattern) {
                return line;
            }
        }
    }
}

fn free_udp_port() -> (UdpSocket, u16) {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    (socket, port)
}

// Runs the guest `wat` on node B, until it prints `success`.
fn run_guest(wat: &str, success: &str) {
    let dir = TempDir::new().unwrap();
    let guest = wat::parse_file(Path::new(env!("CARGO_MANIFEST_DIR")).join(wat)).unwrap();
    std::fs::write(dir.path().join("guest.wasm"), guest).unwrap();

    let (sender, lines) = mpsc::channel();
    let mut output = Output {
        lines,
        seen: Vec::new(),
        deadline: Instant::now() + TIMEOUT,
    };

    let _control = Lunatic::start(dir.path(), "control", &["control"], &sender);
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
    let _node_a = Lunatic::start(
        dir.path(),
        "node A",
        &["node", "--bind-socket", &node_a, &control_url],
        &sender,
    );
    output.wait_for("node A", "Registration successful");

    let node_b = format!("127.0.0.1:{port_b}");
    let _node_b = Lunatic::start(
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
    output.wait_for("node B", success);
}

// Spawns processes on the other node and exchanges messages with them (`wat/distributed.wat`).
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
