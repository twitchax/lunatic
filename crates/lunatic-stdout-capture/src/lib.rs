use std::{
    fmt::{Display, Formatter},
    io::{self, stdout, Cursor, Write},
    pin::Pin,
    sync::{Arc, Mutex, RwLock},
    task::{Context, Poll},
};

use bytes::Bytes;
use tokio::io::AsyncWrite;
use wasmtime_wasi::{
    cli::{IsTerminal, StdoutStream},
    p2::{OutputStream, Pollable, StreamError, StreamResult},
};

// The number of bytes a single write may carry, matching wasmtime-wasi's own stdio streams.
const WRITE_BUDGET: usize = 64 * 1024;

// This signature looks scary, but it just means that the vector holding all output streams
// is rarely extended and often accessed (`RwLock`). The `Mutex` is necessary to allow
// parallel writes for independent processes, it doesn't have any contention.
type StdOutVec = Arc<RwLock<Vec<Mutex<Cursor<Vec<u8>>>>>>;

/// `StdoutCapture` holds the standard output from multiple processes.
///
/// The most common pattern of usage is to capture together the output from a starting process
/// and all sub-processes. E.g. Hide output of sub-processes during testing.
#[derive(Clone, Debug)]
pub struct StdoutCapture {
    // If true, all captured writes are echoed to stdout. This is used in testing scenarios with
    // the flag `--nocapture` set, because we still need to capture the output to inspect panics.
    echo: bool,
    writers: StdOutVec,
    // Index of the stdout currently in use by a process
    index: usize,
}

impl PartialEq for StdoutCapture {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.writers, &other.writers) && self.index == other.index
    }
}

// Displays content of all processes contained inside `StdoutCapture`.
impl Display for StdoutCapture {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let streams = RwLock::read(&self.writers).unwrap();
        // If there is only one process, don't enumerate the output
        if streams.len() == 1 {
            write!(f, "{}", self.content()).unwrap();
        } else {
            for (i, stream) in streams.iter().enumerate() {
                writeln!(f, " --- process {i} stdout ---").unwrap();
                let stream = stream.lock().unwrap();
                let content = String::from_utf8_lossy(stream.get_ref()).to_string();
                write!(f, "{content}").unwrap();
            }
        }
        Ok(())
    }
}

impl StdoutCapture {
    // Create a new `StdoutCapture` with one stream inside.
    pub fn new(echo: bool) -> Self {
        Self {
            echo,
            writers: Arc::new(RwLock::new(vec![Mutex::new(Cursor::new(Vec::new()))])),
            index: 0,
        }
    }

    /// Returns `true` if this is the only reference to the outputs.
    pub fn only_reference(&self) -> bool {
        Arc::strong_count(&self.writers) == 1
    }

    /// Returns a clone of `StdoutCapture` pointing to the next stream
    pub fn next(&self) -> Self {
        let index = {
            let mut writers = RwLock::write(&self.writers).unwrap();
            // If the stream already exists don't add a new one, e.g. stdout & stderr share the same stream.
            writers.push(Mutex::new(Cursor::new(Vec::new())));
            writers.len() - 1
        };
        Self {
            echo: self.echo,
            writers: self.writers.clone(),
            index,
        }
    }

    /// Returns true if all streams are empty
    pub fn is_empty(&self) -> bool {
        let streams = RwLock::read(&self.writers).unwrap();
        streams.iter().all(|stream| {
            let stream = stream.lock().unwrap();
            stream.get_ref().is_empty()
        })
    }

    /// Returns stream's content
    pub fn content(&self) -> String {
        let streams = RwLock::read(&self.writers).unwrap();
        let stream = streams[self.index].lock().unwrap();
        String::from_utf8_lossy(stream.get_ref()).to_string()
    }

    /// Add string to end of the stream
    pub fn push_str(&self, content: &str) {
        let streams = RwLock::read(&self.writers).unwrap();
        let mut stream = streams[self.index].lock().unwrap();
        write!(stream, "{content}").unwrap();
    }

    // Write guest output to the stream, echoing it to stdout if requested.
    fn write_captured(&self, buf: &[u8]) -> io::Result<()> {
        let streams = RwLock::read(&self.writers).unwrap();
        let mut stream = streams[self.index].lock().unwrap();
        stream.write_all(buf)?;
        if self.echo {
            stdout().write_all(buf)?;
        }
        Ok(())
    }
}

impl IsTerminal for StdoutCapture {
    fn is_terminal(&self) -> bool {
        false
    }
}

impl StdoutStream for StdoutCapture {
    fn p2_stream(&self) -> Box<dyn OutputStream> {
        Box::new(self.clone())
    }

    fn async_stream(&self) -> Box<dyn AsyncWrite + Send + Sync> {
        Box::new(self.clone())
    }
}

// Writes are synchronous, so captured output is visible as soon as the guest's write returns.
impl OutputStream for StdoutCapture {
    fn write(&mut self, bytes: Bytes) -> StreamResult<()> {
        self.write_captured(&bytes)
            .map_err(|e| StreamError::LastOperationFailed(e.into()))
    }

    fn flush(&mut self) -> StreamResult<()> {
        Ok(())
    }

    fn check_write(&mut self) -> StreamResult<usize> {
        Ok(WRITE_BUDGET)
    }
}

#[wasmtime_wasi::async_trait]
impl Pollable for StdoutCapture {
    async fn ready(&mut self) {}
}

impl AsyncWrite for StdoutCapture {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write_captured(buf).map(|()| buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
