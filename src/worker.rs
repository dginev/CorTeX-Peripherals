// Copyright 2015 Deyan Ginev. See the LICENSE
// file at the top-level directory of this distribution.
//
// Licensed under the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! base class automating dispatcher communication via ZMQ

use std::borrow::Cow;
use std::error::Error;
use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::ops::Deref;
use std::path::Path;
use std::thread;
use std::time::Duration;

use tempfile::{Builder, TempDir};
use zmq::{Context, Message, Socket, SNDMORE};

/// A task leased from the ventilator: `(input_filepath, input_size, taskid)` — the
/// source archive's on-disk path, its byte size, and the task id whose result must
/// be returned to the sink.
pub type LeasedTask = (String, usize, String);

/// Generic requirements for CorTeX workers
pub trait Worker: Clone + Send {
    /// Core processing method
    fn convert(&self, _: &Path) -> Result<File, Box<dyn Error>>;
    /// Size of chunk for network communication, larger implies less IO, smaller implies less RAM use
    fn message_size(&self) -> usize;
    /// Name of the service, as registered in CorTeX
    fn get_service(&self) -> &str;
    /// URL to the CorTeX dispatcher
    fn get_source_address(&self) -> Cow<'_, str>;
    /// URL to the CorTeX sink
    fn get_sink_address(&self) -> Cow<'_, str>;
    /// Simultaneous threads used for one worker each
    fn pool_size(&self) -> usize {
        1
    }
    /// Sets a uniquely identifying string for this worker instance
    fn set_identity(&mut self, _identity: String) {
        unimplemented!()
    }
    /// Gets the uniquely identifying string of this worker instance
    fn get_identity(&self) -> &str {
        unimplemented!()
    }

    /// sets up the worker process, with as many threads as requested
    fn start(&mut self, limit: Option<usize>) -> Result<(), Box<dyn Error>>
    where
        Self: 'static + Sized,
    {
        let hostname = hostname::get()
            .unwrap_or_else(|_| OsString::from("hostname"))
            .into_string()
            .unwrap_or_else(|_| "hostname".to_string());
        // The worker's ZMQ identity is `<hostname>:<service>:<pid>[-<thread>]`.
        // CorTeX records it verbatim in `worker_metadata` (the per-worker
        // dispatch/return tallies and fleet topology on the dashboard), so it
        // MUST carry the real service name (it was once hardcoded to a fixed
        // legacy string, mislabeling every other service).
        //
        // It MUST also be GLOBALLY UNIQUE across the whole fleet — this is
        // correctness, not cosmetics. The dispatcher's ROUTER routes task
        // replies by identity with `router_handover` enabled, so any two
        // workers sharing an identity are treated as ONE peer: a second
        // connection hijacks the first's identity, the ventilator's replies are
        // delivered to whichever connection currently owns it (or dropped if the
        // owner just reconnected), and the rest starve while their leased tasks
        // leak into the in-flight set until the lease reaper reclaims them. The
        // common fleet layout is many *single-worker processes* (one conversion
        // per process, for clean per-paper RAM/timeout isolation), so the PID is
        // what distinguishes them; the thread suffix additionally distinguishes
        // pooled threads inside one process.
        let service = self.get_service().to_string();
        let pid = std::process::id();
        match self.pool_size() {
            1 => {
                self.set_identity(format!("{hostname}:{service}:{pid}"));
                self.start_single(limit)
            }
            n => {
                let mut threads = Vec::new();
                for thread in 1..=n {
                    let identity_single = format!("{hostname}:{service}:{pid}-{thread:02}");
                    let mut thread_self: Self = self.clone();
                    thread_self.set_identity(identity_single);
                    threads.push(thread::spawn(move || {
                        // A single worker thread must not take the whole pool down on a
                        // transient fault: log and let the siblings keep running (the
                        // dispatcher's lease reaper recovers any task the exited thread
                        // was holding). Replaces the old `.unwrap()` that aborted the
                        // process on the first thread error.
                        if let Err(e) = thread_self.start_single(limit) {
                            error!(
                              target: "pericortex:worker",
                              "worker thread {thread} exited with error: {e}"
                            );
                        }
                    }));
                }
                for t in threads {
                    if let Err(e) = t.join() {
                        error!(target: "pericortex:worker", "worker thread panicked: {e:?}");
                    }
                }
                Ok(())
            }
        }
    }
    /// (Re)build the DEALER request socket: a fresh socket carrying our identity,
    /// dialed at the ventilator, with bounded send/recv timeouts.
    ///
    /// Called at startup and to **recover from a wedged exchange**. A dispatcher
    /// that crashed (or is mid-restart) *after* we sent a request but *before* it
    /// replied would otherwise leave a blocking `recv` hung forever. Rebuilding
    /// atomically discards the lost request **and** any late reply, so the
    /// request→reply pairing can never desync — a stray reply on the old socket
    /// can't be misread as the *next* task's id (the "wires crossing" failure
    /// mode). The receive/send timeouts ([`worker_io_timeout_ms`]) are what make
    /// such a wedge *detectable*: a healthy ventilator replies to every request
    /// immediately (even "no work" is an instant mock reply), so a multi-second
    /// silence is always a fault, never normal backpressure.
    fn connect_source(&self, context: &Context) -> Result<Socket, Box<dyn Error>> {
        let source = context.socket(zmq::DEALER)?;
        source.set_identity(self.get_identity().as_bytes())?;
        let io_timeout_ms = worker_io_timeout_ms();
        source.set_rcvtimeo(io_timeout_ms)?;
        source.set_sndtimeo(io_timeout_ms)?;
        // libzmq `connect` is asynchronous — it only validates the endpoint
        // string here and dials in the background, so a temporarily-down
        // dispatcher is *not* an error: the socket reconnects when it returns.
        source.connect(&self.get_source_address())?;
        Ok(source)
    }

    /// Back off and re-request when the dispatcher has no work for us. Keeps an
    /// idle worker from busy-spinning on the ventilator. Configurable via
    /// `CORTEX_WORKER_THROTTLE_SECS` (default 60) — lower it for fast services or
    /// tests where a 60 s nap dominates the tail (once a corpus drains, an idle
    /// worker otherwise sleeps a full minute before noticing fresh work).
    fn throttle(&self) {
        let throttle_secs = std::env::var("CORTEX_WORKER_THROTTLE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(60);
        info!(
          target: &format!("{}:idle", self.get_identity()),
          "no work available; throttling {throttle_secs}s before re-requesting"
        );
        thread::sleep(Duration::new(throttle_secs, 0));
    }

    /// main worker loop for a single thread, works in perpetuity or up to a specified `limit`
    ///
    /// Robustness contract (the worker assumes "anything that can go wrong will"):
    /// no transient fault — a transport hiccup, a wedged/lost reply, a full disk,
    /// a panicking conversion (the [`Worker::convert`] impl owns that) — takes the
    /// worker down. Every failure is logged and turned into either a re-request,
    /// a socket rebuild, or a returned-failure for the dispatcher to record. The
    /// process only exits on `limit` completion or an unrecoverable setup error.
    fn start_single(&self, limit: Option<usize>) -> Result<(), Box<dyn Error>> {
        let mut work_counter = 0;
        // Request socket (DEALER → ventilator). Rebuilt on a wedged exchange.
        let context_source = Context::new();
        let mut source = self.connect_source(&context_source)?;
        // Result socket (PUSH → sink). A bounded send timeout means a gone sink
        // can't block the worker forever: a failed return is logged and the task
        // is recovered by the dispatcher's lease reaper.
        let context_sink = Context::new();
        let sink = context_sink.socket(zmq::PUSH)?;
        sink.set_sndtimeo(worker_io_timeout_ms())?;
        sink.connect(&self.get_sink_address())?;

        loop {
            // Per-task scratch dir. A transient inability to make one (full /tmp,
            // fd exhaustion) must NOT kill a long-running fleet worker — back off
            // briefly and retry rather than `.unwrap()`-panic. `TempDir` removes
            // itself on drop at the end of each iteration.
            let input_tmpdir = match Builder::new().prefix("cortex_task").tempdir() {
                Ok(dir) => dir,
                Err(e) => {
                    error!(
                      target: &format!("{}:error", self.get_identity()),
                      "could not create a scratch tempdir ({e}); backing off 5s and retrying"
                    );
                    thread::sleep(Duration::new(5, 0));
                    continue;
                }
            };

            match self.receive_from_cortex(&input_tmpdir, &source) {
                Ok(Some((input_filepath, _input_size, taskid))) => {
                    // A real task was leased. The worker's `convert` owns *its*
                    // failure isolation (a robust impl always returns either a
                    // result archive or a structured-failure archive, never
                    // panics), so we just stream whatever it returns back.
                    let converted = self.convert(Path::new(&input_filepath));
                    if let Err(e) = self.respond_to_cortex(converted, &taskid, &sink) {
                        // Sink unreachable mid-return: the result is lost but the
                        // task is recovered by the dispatcher's reaper. Don't die.
                        error!(
                          target: &format!("{}:result", self.get_identity()),
                          "could not return task {taskid} to the sink ({e}); the dispatcher will re-lease it"
                        );
                    }
                    // Only *real* tasks advance a bounded run — empty/mock replies
                    // and faults must not consume the `--limit` budget (otherwise a
                    // momentarily-empty queue would "finish" a bounded run early).
                    work_counter += 1;
                    if let Some(upper_bound) = limit
                        && work_counter >= upper_bound
                    {
                        // Give the final result a moment to flush to the sink.
                        thread::sleep(Duration::new(1, 0));
                        break;
                    }
                },
                Ok(None) => {
                    // The dispatcher had nothing for us (a mock "0" reply: empty
                    // queue, backpressure, paused corpus, or unknown service).
                    self.throttle();
                },
                Err(e) => {
                    // Transport fault or a wedged/timed-out reply. Rebuild the
                    // request socket (discards the in-flight request + any late
                    // reply — desync-proof) and retry after a short backoff.
                    error!(
                      target: &format!("{}:source", self.get_identity()),
                      "request socket fault ({e}); rebuilding and retrying"
                    );
                    thread::sleep(Duration::new(TRANSPORT_BACKOFF_SECS, 0));
                    match self.connect_source(&context_source) {
                        Ok(s) => source = s,
                        Err(e2) => error!(
                          target: &format!("{}:source", self.get_identity()),
                          "could not rebuild the request socket ({e2}); will retry next loop"
                        ),
                    }
                },
            }
        }
        Ok(())
    }

    /// Request one task from the ventilator and stream its source archive to
    /// `input_tmpdir`.
    ///
    /// Returns:
    /// * `Ok(Some((path, size, taskid)))` — a real task; its source archive is at
    ///   `path` (`size` bytes), to be returned to the sink under `taskid`. A
    ///   real task with a 0-byte payload is still returned (the dispatcher must
    ///   record the failure, not leave it in-flight).
    /// * `Ok(None)` — a mock `"0"` reply: no work available. The caller throttles.
    /// * `Err(_)` — a transport fault or a timed-out reply. The caller rebuilds
    ///   the socket. **No partial state escapes** — a desync can't leak into the
    ///   next request because the socket is discarded.
    fn receive_from_cortex(
        &self,
        input_tmpdir: &TempDir,
        source: &Socket,
    ) -> Result<Option<LeasedTask>, Box<dyn Error>> {
        // Ask the ventilator for a task for our service; the ROUTER prepends our
        // identity, so it sees `[identity, service]`.
        source.send(self.get_service(), 0)?;

        // First reply frame is the task id ("0" = a mock "no work" reply). A
        // `recv` that times out (RCVTIMEO) surfaces as `Err` → the caller rebuilds.
        let mut taskid_msg = Message::new();
        source.recv(&mut taskid_msg, 0)?;
        let taskid = taskid_msg.as_str().unwrap_or("0").to_string();
        let is_real_task = taskid != "0" && taskid != "-1";

        // The reply is multipart `[taskid, data...]`; the ventilator always
        // appends ≥1 data frame (even an empty one). Drain *every* data frame so
        // the socket stays message-aligned, streaming a real task's bytes to disk
        // one frame at a time (bounded memory, regardless of archive size).
        let input_filepath = format!(
            "{}/{}.zip",
            input_tmpdir.path().to_str().unwrap_or("/tmp"),
            taskid
        );
        let mut file = if is_real_task {
            Some(File::create(&input_filepath)?)
        } else {
            None
        };
        let mut input_size = 0usize;
        while source.get_rcvmore()? {
            let mut frame = Message::new();
            source.recv(&mut frame, 0)?;
            if let Some(f) = file.as_mut() {
                // `write_all` so a short write (full disk) is an error, not a
                // silently-truncated archive that would convert to garbage.
                f.write_all(frame.deref())?;
                input_size += frame.len();
            }
        }
        // Flush + close the input before `convert` reopens it by path.
        drop(file);

        if !is_real_task {
            return Ok(None);
        }
        info!(
          target: &format!("{}:received", self.get_identity()),
          "task {taskid}, read {input_size} bytes from CorTeX."
        );
        Ok(Some((input_filepath, input_size, taskid)))
    }

    /// Return one task's result to the sink as `[identity, service, taskid, data…]`
    /// — exactly the envelope the dispatcher's sink expects (it re-validates the
    /// service against the task before accepting, so the frames must be exact).
    ///
    /// Errors (a gone/blocked sink) are propagated, not panicked: the caller logs
    /// and the dispatcher's reaper re-leases the task. The conversion result is
    /// streamed in `message_size` chunks — O(chunk) memory regardless of how large
    /// the output archive is, with correct EOF detection (a short read mid-file no
    /// longer truncates the stream, and an empty result still sends one empty data
    /// frame so the envelope stays well-formed at ≥4 frames).
    fn respond_to_cortex(
        &self,
        file_result: Result<File, Box<dyn Error>>,
        taskid: &str,
        sink: &Socket,
    ) -> Result<(), Box<dyn Error>> {
        sink.send(self.get_identity(), SNDMORE)?;
        sink.send(self.get_service(), SNDMORE)?;
        sink.send(taskid, SNDMORE)?;
        match file_result {
            Ok(mut converted_file) => {
                let message_size = self.message_size().max(1);
                let mut total_size = 0usize;
                // Read-ahead by one chunk so the LAST frame is sent without
                // SNDMORE (true EOF), correctly handling short reads and an
                // exactly-chunk-aligned file.
                let mut pending: Option<Vec<u8>> = None;
                loop {
                    let mut buf = vec![0u8; message_size];
                    let size = converted_file.read(&mut buf)?;
                    if size == 0 {
                        break;
                    }
                    buf.truncate(size);
                    total_size += size;
                    if let Some(prev) = pending.take() {
                        sink.send(&prev, SNDMORE)?;
                    }
                    pending = Some(buf);
                }
                match pending {
                    Some(last) => sink.send(&last, 0)?,
                    // Empty result: still send one (empty) data frame so the
                    // envelope is the well-formed ≥4 frames the sink requires.
                    None => sink.send(Vec::new(), 0)?,
                }
                info!(
                  target: &format!("{}:completed", self.get_identity()),
                  "task {taskid}, sent {total_size} bytes back to CorTeX."
                );
            },
            Err(e) => {
                // The conversion produced nothing usable (an infrastructure
                // failure — disk, OOM-survivor, etc.). Send a single empty data
                // frame so the envelope is well-formed and the dispatcher records
                // the task as failed instead of leaving it in-flight.
                warn!(
                  target: &format!("{}:result", self.get_identity()),
                  "task {taskid} produced no result ({e}); returning an empty (failed) reply"
                );
                sink.send(Vec::new(), 0)?;
            },
        }
        Ok(())
    }
}

/// Per-call ZMQ send/recv timeout (milliseconds) for the worker's request
/// socket, configurable via `CORTEX_WORKER_IO_TIMEOUT_SECS` (default 120 s).
///
/// This is **not** a backpressure or latency knob: a healthy ventilator answers
/// every request immediately, so the only thing this bounds is how long a worker
/// waits on a **dead or wedged** dispatcher before giving up on that exchange and
/// rebuilding its socket. Keep it comfortably above worst-case network round-trip
/// plus the dispatcher's reply latency, so a momentary pause never triggers a
/// needless rebuild.
fn worker_io_timeout_ms() -> i32 {
    std::env::var("CORTEX_WORKER_IO_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<i32>().ok())
        .filter(|&s| s > 0)
        .map(|s| s.saturating_mul(1000))
        .unwrap_or(120_000)
}

/// Backoff (seconds) after a request-socket fault before rebuilding + retrying.
/// Short, so a recovered dispatcher is picked up promptly, but non-zero so a
/// persistently-down dispatcher doesn't spin the rebuild loop.
const TRANSPORT_BACKOFF_SECS: u64 = 2;

mod echo;
pub use echo::EchoWorker;

mod tex_to_html;
pub use tex_to_html::TexToHtmlWorker;
