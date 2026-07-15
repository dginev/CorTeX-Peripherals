// Copyright 2015-2026 Deyan Ginev. See the LICENSE
// file at the top-level directory of this distribution.
//
// Licensed under the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! A process supervisor for a fleet of CorTeX worker processes.
//!
//! The robust deployment model for an in-process converter is **one conversion
//! per process**: each worker runs `--pool-size 1`, so its per-process resource
//! guards (a RAM ceiling, a wall-clock timeout) bound exactly one document, and
//! a timeout / OOM / panic / segfault kills only that one worker — the
//! dispatcher's lease reaper re-leases the single task it was holding, and the
//! harness respawns it. (Running one process with `--pool-size N` instead would
//! share a single RAM ceiling across N concurrent conversions, so the process
//! RSS blows the cap and the memory guards false-positive on every paper.)
//!
//! This is the Rust successor to the historical Perl
//! `LaTeXML-Plugin-Cortex/bin/latexml_harness`: a CPU-derived worker count, a
//! periodic supervision sweep that respawns dead workers, and a clean
//! SIGTERM/SIGINT shutdown that stops the whole fleet.
//!
//! On top of that baseline it adds three properties that matter at scale (a
//! fleet on a many-core box converting an adversarial corpus for days):
//!
//! * **Memory-aware sizing** ([`workers_fitting_memory`]) so the auto-derived
//!   worker count can't blow physical RAM: `workers × mem_limit` is clamped to a
//!   headroom fraction of [`total_ram_bytes`]. A 128-core box would otherwise
//!   spawn ~124 workers, and at a 4 GiB per-child cap that is ~496 GiB of budget
//!   against far less RAM — the exact over-commit that motivated this guard.
//! * **Crash-loop containment** — a worker that dies *fast* and *uncleanly*
//!   (before [`FAST_DEATH_THRESHOLD`], non-zero/​signal exit) is respawned with
//!   exponential backoff (capped at [`MAX_RESPAWN_BACKOFF`]) instead of
//!   hammering `fork`/`exec`. A clean exit (a completed `--limit`) or a slow
//!   death (a real per-paper OOM/timeout/panic after startup) respawns promptly.
//! * **No orphans** — each child sets `PR_SET_PDEATHSIG` so a SIGKILLed or
//!   crashed harness takes its converting children down with it, rather than
//!   leaving unsupervised processes consuming CPU/RAM/sockets.
//!
//! Death detection is SIGCHLD-driven (a dead worker wakes the sweep in ~200 ms
//! rather than after the full poll interval), with [`HarnessConfig::poll`] as
//! the periodic backstop.

use std::error::Error;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Set by the SIGTERM/SIGINT handler; polled by the supervision loop so a
/// shutdown stops leasing and tears the fleet down cleanly.
static STOP: AtomicBool = AtomicBool::new(false);

/// Set by the SIGCHLD handler: a child changed state (most importantly, exited).
/// The supervision loop consumes it to wake from its sleep immediately, so a
/// dead worker is reaped and respawned promptly instead of waiting out the full
/// [`HarnessConfig::poll`] interval. It is only a wake hint — [`Child::try_wait`]
/// remains the source of truth, so a missed/spurious signal only changes
/// latency, never correctness.
static CHILD_DIED: AtomicBool = AtomicBool::new(false);

extern "C" fn on_stop_signal(_sig: libc::c_int) { STOP.store(true, Ordering::SeqCst); }

extern "C" fn on_sigchld(_sig: libc::c_int) { CHILD_DIED.store(true, Ordering::SeqCst); }

/// Install the SIGTERM/SIGINT (shutdown) and SIGCHLD (wake) handlers
/// (idempotent). After this, [`stop_requested`] reports whether a shutdown
/// signal has arrived. The handlers only perform an atomic store, which is
/// async-signal-safe.
fn install_signal_handlers() {
  // Cast via an explicit `extern "C" fn` pointer first (a bare fn-item-to-integer
  // cast is a clippy footgun).
  let stop = on_stop_signal as extern "C" fn(libc::c_int) as libc::sighandler_t;
  let chld = on_sigchld as extern "C" fn(libc::c_int) as libc::sighandler_t;
  // Safe: the handlers only do an atomic store. We deliberately install a real
  // handler for SIGCHLD (not SIG_IGN / SA_NOCLDWAIT) so the kernel does NOT
  // auto-reap — `try_wait`/`wait` stay authoritative and exit statuses survive.
  unsafe {
    libc::signal(libc::SIGTERM, stop);
    libc::signal(libc::SIGINT, stop);
    libc::signal(libc::SIGCHLD, chld);
  }
}

/// Whether a shutdown signal (SIGTERM/SIGINT) has been received.
#[must_use]
pub fn stop_requested() -> bool { STOP.load(Ordering::SeqCst) }

/// The default number of worker processes by **CPU**: leave **1–4 logical
/// cores** for the OS + dispatcher, mirroring the historical Perl harness's
/// reservation policy (subtract 4 above 16 cores, 2 above 4, 1 above 2). Never
/// returns 0.
///
/// This is only the CPU bound. Before using it to size a fleet with a per-child
/// memory cap, pass it through [`workers_fitting_memory`] so the aggregate can't
/// over-commit physical RAM.
#[must_use]
pub fn default_worker_count() -> usize {
  let cpus = thread::available_parallelism().map_or(1, |n| n.get());
  let reserve = if cpus > 16 {
    4
  } else if cpus > 4 {
    2
  } else if cpus > 2 {
    1
  } else {
    0
  };
  cpus.saturating_sub(reserve).max(1)
}

/// Total physical RAM in **bytes**, parsed from `/proc/meminfo` `MemTotal`.
/// `None` if it can't be read (non-Linux, or `/proc` unavailable) — callers then
/// skip RAM-based clamping and fall back to the CPU bound.
#[must_use]
pub fn total_ram_bytes() -> Option<u64> {
  let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
  for line in meminfo.lines() {
    if let Some(rest) = line.strip_prefix("MemTotal:") {
      // e.g. "MemTotal:       258734004 kB"
      let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
      return Some(kb.saturating_mul(1024));
    }
  }
  None
}

/// Available (allocatable without swapping) RAM in **bytes**, from
/// `/proc/meminfo` `MemAvailable` — the kernel's own estimate, which already
/// accounts for reclaimable page cache. `None` if it can't be read. This is the
/// live signal the memory-pressure governor watches: it reflects *all* memory
/// users (every worker, their graphics subprocesses, the OS), not just a sum of
/// worker RSS, so it is the honest "are we about to OOM the host" measure.
#[must_use]
pub fn available_ram_bytes() -> Option<u64> {
  let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
  for line in meminfo.lines() {
    if let Some(rest) = line.strip_prefix("MemAvailable:") {
      let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
      return Some(kb.saturating_mul(1024));
    }
  }
  None
}

/// Resident set size of a child process in **bytes**, from
/// `/proc/<pid>/status` `VmRSS`. `None` if the process is gone or unreadable.
/// Used by the governor to pick the largest worker to shed under pressure.
fn child_rss_bytes(pid: u32) -> Option<u64> {
  let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
  for line in status.lines() {
    if let Some(rest) = line.strip_prefix("VmRSS:") {
      let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
      return Some(kb.saturating_mul(1024));
    }
  }
  None
}

/// Percent of physical RAM the fleet may budget for workers, leaving the rest
/// for the OS, page cache, and the supervisor itself. Overridable via
/// `CORTEX_HARNESS_RAM_HEADROOM_PCT` (clamped to 10..=100); default 85.
fn ram_budget_percent() -> u64 {
  std::env::var("CORTEX_HARNESS_RAM_HEADROOM_PCT")
    .ok()
    .and_then(|v| v.parse::<u64>().ok())
    .map(|p| p.clamp(10, 100))
    .unwrap_or(85)
}

/// Clamp a requested worker count so the fleet's aggregate per-child memory
/// ceiling fits within a headroom fraction (see `CORTEX_HARNESS_RAM_HEADROOM_PCT`,
/// default 85%) of physical RAM:
///
/// `min(requested, floor(usable_ram / mem_limit_bytes))`, never below 1.
///
/// Returns `requested` unchanged when there is no per-child cap
/// (`mem_limit_bytes` is `None`/0) or physical RAM can't be read. Because
/// `mem_limit_bytes` caps **address space (VSZ)** — which an arena allocator
/// like mimalloc runs *above* true RSS — sizing by it is deliberately
/// conservative: the real resident footprint per worker sits below the cap, so
/// the fleet has headroom even when several workers hit heavy papers at once.
/// Operators who have measured a lower steady-state RSS can raise the count past
/// this bound explicitly (e.g. cortex_worker's `--workers`).
#[must_use]
pub fn workers_fitting_memory(requested: usize, mem_limit_bytes: Option<u64>) -> usize {
  let Some(per_child) = mem_limit_bytes.filter(|&b| b > 0) else {
    return requested.max(1);
  };
  let Some(total) = total_ram_bytes() else {
    return requested.max(1);
  };
  // `total / 100 * pct` (divide first) keeps the product well clear of overflow
  // for any realistic RAM size.
  let usable = total / 100 * ram_budget_percent();
  let by_ram = (usable / per_child).max(1) as usize;
  requested.min(by_ram).max(1)
}

/// Tunables for [`supervise`].
pub struct HarnessConfig {
  /// Number of worker processes to keep alive. Size it with
  /// [`workers_fitting_memory`] when [`mem_limit_bytes`](Self::mem_limit_bytes)
  /// is set so the fleet can't over-commit RAM.
  pub workers: usize,
  /// How often the supervision loop sweeps for dead workers. This is only the
  /// periodic backstop — SIGCHLD wakes the sweep as soon as a worker dies, so
  /// respawn latency is normally far below this interval.
  pub poll: Duration,
  /// Base backoff before respawning a just-exited worker. A worker that keeps
  /// dying *fast and uncleanly* (a crash loop) backs off exponentially from this
  /// base up to [`MAX_RESPAWN_BACKOFF`]; a clean or slow exit respawns after just
  /// this base delay.
  pub respawn_backoff: Duration,
  /// Per-child virtual-address-space ceiling in **bytes**, applied with
  /// `setrlimit(RLIMIT_AS)` in each spawned worker *before* `exec` (`None` =
  /// no limit). When the child later allocates past it, the allocation fails
  /// with `ENOMEM`; a worker built around an alloc-error hook turns that into a
  /// clean `Fatal:oom` + non-zero exit, and the supervisor respawns it — so a
  /// memory runaway costs exactly one paper, attributably, never the fleet.
  ///
  /// NB: this bounds **address space (VSZ)**, not resident RAM (RSS) — the only
  /// `setrlimit` knob that constrains memory portably without privileges
  /// (`RLIMIT_RSS` is a no-op on Linux; a true RSS cap needs cgroups). With an
  /// arena allocator (e.g. mimalloc) VSZ runs above RSS, so the real resident
  /// kill point sits *below* this number; set it at or above the intended RAM
  /// ceiling. The limit is only ever lowered, never raised above any ambient
  /// hard limit (raising a hard limit needs privilege).
  pub mem_limit_bytes: Option<u64>,
  /// Floor in **bytes** for the fleet memory-pressure governor (`None` =
  /// governor off). While system [`available_ram_bytes`] is below this floor,
  /// the harness sheds its **largest-RSS** worker (SIGTERM — the dispatcher
  /// re-leases that one paper) and suppresses respawns, so the fleet shrinks
  /// until memory recovers past a hysteresis ceiling (1.5× the floor), then
  /// refills. This is what makes deliberate over-commit safe: the per-child
  /// `mem_limit_bytes` contains a *single* runaway, while this governor contains
  /// the *aggregate* — a rare cluster of simultaneously-heavy (even legitimate)
  /// jobs — with a deliberate, attributable, recoverable shed instead of an
  /// uncontrolled kernel OOM-kill that might fell the dispatcher or the harness
  /// itself. The common case (most jobs small) never trips it and runs the full
  /// worker count. Pair it with an outer cgroup `memory.max` for a hard backstop.
  pub mem_pressure_floor_bytes: Option<u64>,
  /// If set, a live worker whose **CPU time stops advancing** for this long is
  /// treated as *unresponsive* — wedged in a blocking wait / deadlocked, or a
  /// task whose own in-process watchdog failed to fire — and SIGKILLed so the
  /// slot reaps and respawns (`None` = check disabled). Death-driven supervision
  /// (SIGCHLD) can't see this: the process is still *alive*, just making no
  /// progress. Detection is CPU-progress based (reads `/proc/<pid>/stat`), so it
  /// catches a *frozen* worker; a rare busy-**spinning** wedge (CPU advancing but
  /// no real progress) is left to the task's own watchdog. Set it comfortably
  /// **above** the worker's per-task wall-clock timeout — a legitimately slow
  /// task blocked on a subprocess with the worker's own CPU idle would otherwise
  /// be a false positive. A killed wedge is a *slow* death (respawns promptly, no
  /// crash-loop backoff); its in-flight task is re-leased by the dispatcher.
  pub unresponsive_timeout: Option<Duration>,
}

impl Default for HarnessConfig {
  fn default() -> Self {
    HarnessConfig {
      workers: default_worker_count(),
      poll: Duration::from_secs(10),
      respawn_backoff: Duration::from_secs(1),
      mem_limit_bytes: None,
      mem_pressure_floor_bytes: None,
      unresponsive_timeout: None,
    }
  }
}

/// A worker that exits sooner than this after being spawned, with a non-clean
/// status, is treated as a crash loop (it almost certainly never processed a
/// paper — startup alone is sub-second) and backed off. Above it, an unclean
/// exit is read as a genuine per-paper failure (OOM/timeout/panic) after real
/// work, and respawns promptly.
const FAST_DEATH_THRESHOLD: Duration = Duration::from_secs(5);

/// Ceiling on the exponential respawn backoff, so a permanently-broken slot
/// retries about once a minute rather than wedging the fleet or spinning.
const MAX_RESPAWN_BACKOFF: Duration = Duration::from_secs(60);

/// Minimum gap between successive sheds, so the governor frees memory one worker
/// at a time and lets each kill settle (the victim takes a moment to actually
/// release pages) before deciding whether more shedding is needed — rather than
/// culling the whole fleet in a single pressure spike.
const SHED_INTERVAL: Duration = Duration::from_secs(3);

/// Sweep cadence while under memory pressure: short, so the governor reacts to a
/// fast-growing heavy cluster (and notices recovery) well inside the normal
/// poll interval.
const SHED_POLL: Duration = Duration::from_secs(1);

/// Bytes per MiB — for human-readable memory logging.
const MIB: u64 = 1024 * 1024;

/// Per-worker-slot supervision state.
struct Slot {
  /// The live child, or `None` while the slot is empty (just died, or waiting
  /// out a respawn backoff).
  child: Option<Child>,
  /// When the current child was spawned — for fast-death detection.
  spawned_at: Instant,
  /// Consecutive fast+unclean deaths; drives the exponential backoff. Reset on
  /// any clean or slow exit.
  fast_deaths: u32,
  /// Earliest instant this slot may be respawned. `None` = eligible now.
  respawn_after: Option<Instant>,
  /// Total CPU ticks (utime+stime) at the last sweep, for the unresponsive
  /// (no-CPU-progress) watchdog. `None` until first sampled after a spawn.
  last_cpu_ticks: Option<u64>,
  /// Instant the child's CPU time was last seen to advance (or when spawned).
  /// CPU frozen past `unresponsive_timeout` from here ⇒ the worker is wedged.
  cpu_advanced_at: Instant,
}

/// Backoff before respawning a slot with `fast_deaths` consecutive fast+unclean
/// deaths: `base`, then doubling per additional death, capped at
/// [`MAX_RESPAWN_BACKOFF`]. `fast_deaths` of 0 or 1 yields `base`.
fn respawn_delay(base: Duration, fast_deaths: u32) -> Duration {
  let shift = fast_deaths.saturating_sub(1).min(20);
  base.saturating_mul(1u32 << shift).min(MAX_RESPAWN_BACKOFF)
}

/// Parse total CPU time (utime + stime, in clock ticks) out of the contents of
/// a `/proc/<pid>/stat` line. Returns `None` if unparseable. The `comm` field
/// (2nd) can contain spaces *and* parentheses, so we anchor on the **final** `)`
/// that closes it and index the numeric fields from there: after that paren the
/// whitespace-split tokens begin at field 3 (`state`), so `utime` (field 14) is
/// index 11 and `stime` (field 15) is index 12.
fn parse_cpu_ticks(stat: &str) -> Option<u64> {
  let after_comm = &stat[stat.rfind(')')? + 1..];
  let fields: Vec<&str> = after_comm.split_whitespace().collect();
  let utime: u64 = fields.get(11)?.parse().ok()?;
  let stime: u64 = fields.get(12)?.parse().ok()?;
  Some(utime.saturating_add(stime))
}

/// Total CPU ticks consumed by `pid` so far, from `/proc/<pid>/stat`. `None` if
/// the process is gone or `/proc` is unreadable (e.g. a non-Linux target).
fn child_cpu_ticks(pid: u32) -> Option<u64> {
  parse_cpu_ticks(&std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?)
}

/// Send SIGTERM to a child (graceful relative to SIGKILL: the kernel still
/// closes the worker's sockets on exit), rather than `Child::kill`'s SIGKILL.
fn term(child: &Child) {
  // Safe: `kill(2)` with a valid pid + signal; a stale pid just returns ESRCH.
  unsafe {
    libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
  }
}

/// Lower the calling process's `RLIMIT_AS` (virtual address space) to `bytes`,
/// clamped so an ambient hard limit is honoured (a hard limit can only be
/// lowered unprivileged, never raised). Intended to run inside
/// [`Command::pre_exec`]: it performs only the async-signal-safe `getrlimit` /
/// `setrlimit` syscalls and allocates nothing, so it is safe in the fragile
/// post-`fork`, pre-`exec` window.
fn set_address_space_limit(bytes: u64) -> std::io::Result<()> {
  // Safe: plain syscalls on a stack-local `rlimit`; no shared state touched.
  unsafe {
    let mut cur = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
    if libc::getrlimit(libc::RLIMIT_AS, &mut cur) != 0 {
      return Err(std::io::Error::last_os_error());
    }
    let want = bytes as libc::rlim_t;
    // Don't try to exceed an existing hard cap (would EPERM); only constrain.
    let limit = if cur.rlim_max == libc::RLIM_INFINITY {
      want
    } else {
      cur.rlim_max.min(want)
    };
    let rlim = libc::rlimit {
      rlim_cur: limit,
      rlim_max: limit,
    };
    if libc::setrlimit(libc::RLIMIT_AS, &rlim) != 0 {
      return Err(std::io::Error::last_os_error());
    }
  }
  Ok(())
}

/// Ask the kernel to deliver SIGTERM to this (child) process when its parent —
/// the supervising harness — dies, so a SIGKILLed/crashed harness never leaves
/// orphaned converting workers behind. Linux-only (`prctl(PR_SET_PDEATHSIG)`);
/// a no-op elsewhere. Async-signal-safe (one syscall, no allocation) so it is
/// safe in [`Command::pre_exec`].
///
/// Sound here because the harness's spawning thread runs [`supervise`] for the
/// whole process lifetime: the documented PDEATHSIG gotcha (firing when the
/// spawning *thread*, not the process, exits) can't trigger early — that thread
/// only exits at process teardown, by which point a graceful shutdown has
/// already reaped the children.
#[cfg(target_os = "linux")]
fn set_parent_death_signal() -> std::io::Result<()> {
  // Safe: a single `prctl` syscall with constant arguments.
  let rc = unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM as libc::c_ulong, 0, 0, 0) };
  if rc != 0 {
    return Err(std::io::Error::last_os_error());
  }
  Ok(())
}

#[cfg(not(target_os = "linux"))]
fn set_parent_death_signal() -> std::io::Result<()> { Ok(()) }

/// Spawn one worker, logging (but not failing the fleet) on a spawn error so a
/// transient `fork`/`exec` failure is retried on the next sweep. Each child,
/// before it execs, sets `PR_SET_PDEATHSIG` (die with the harness) and — when
/// `mem_limit_bytes` is set — its `RLIMIT_AS` cap.
fn spawn_one<F>(build: &F, index: usize, mem_limit_bytes: Option<u64>) -> Option<Child>
where
  F: Fn(usize) -> Command,
{
  let mut cmd = build(index);
  // Safe: the hook runs in the forked child before `exec` and calls only
  // async-signal-safe helpers (`prctl`/`getrlimit`/`setrlimit` — syscalls, no
  // heap/locks).
  unsafe {
    cmd.pre_exec(move || {
      set_parent_death_signal()?;
      if let Some(bytes) = mem_limit_bytes {
        set_address_space_limit(bytes)?;
      }
      Ok(())
    });
  }
  match cmd.spawn() {
    Ok(child) => {
      info!(target: "pericortex:harness", "spawned worker #{index} (pid {})", child.id());
      Some(child)
    },
    Err(e) => {
      error!(target: "pericortex:harness", "failed to spawn worker #{index}: {e}");
      None
    },
  }
}

/// Record a worker's death into its slot: classify it (clean/slow → reset
/// backoff; fast+unclean → escalate) and schedule the next respawn instant.
fn record_death(slot: &mut Slot, index: usize, exit: Option<ExitStatus>, base: Duration) {
  let now = Instant::now();
  let ran_for = now.saturating_duration_since(slot.spawned_at);
  let clean_exit = matches!(exit, Some(s) if s.success());
  // A crash loop is a non-clean exit that happened too fast to have done useful
  // work. A clean exit (a completed `--limit`) or a slow death (a real per-paper
  // OOM/timeout/panic after startup) is *not* a crash loop.
  let crashed_fast = !clean_exit && ran_for < FAST_DEATH_THRESHOLD;
  if crashed_fast {
    slot.fast_deaths = slot.fast_deaths.saturating_add(1);
  } else {
    slot.fast_deaths = 0;
  }
  let delay = respawn_delay(base, slot.fast_deaths);
  slot.respawn_after = Some(now + delay);
  if crashed_fast {
    warn!(
      target: "pericortex:harness",
      "worker #{index} died fast after {:.1}s (exit {:?}); consecutive fast deaths={}, backing off {:.1}s",
      ran_for.as_secs_f64(), exit, slot.fast_deaths, delay.as_secs_f64()
    );
  } else {
    info!(
      target: "pericortex:harness",
      "worker #{index} exited after {:.1}s (exit {:?}); respawning in {:.1}s",
      ran_for.as_secs_f64(), exit, delay.as_secs_f64()
    );
  }
}

/// The live worker with the highest RSS — the governor's shed victim under
/// memory pressure (freeing the most memory per kill minimises total kills).
/// Returns `(slot_index, pid, rss_bytes)`, or `None` if no live worker's RSS is
/// readable.
fn largest_rss_worker(slots: &[Slot]) -> Option<(usize, u32, u64)> {
  slots
    .iter()
    .enumerate()
    .filter_map(|(i, slot)| slot.child.as_ref().map(|c| (i, c.id())))
    .filter_map(|(i, pid)| child_rss_bytes(pid).map(|rss| (i, pid, rss)))
    .max_by_key(|&(_, _, rss)| rss)
}

/// The fleet's **own** resident footprint: `(summed child RSS in bytes, live
/// worker count)`. This is the only memory a shed can actually free, so it is
/// what tells pressure *we* caused apart from pressure a co-tenant caused.
///
/// The governor watches system-wide `MemAvailable` — deliberately, since that is
/// the honest "are we about to OOM the host" measure — but that also means any
/// foreign process can drive the fleet into shedding work it will never get back.
/// Without this number in the log, a co-tenant's leak is indistinguishable from a
/// conversion regression: it took a full day to tell those apart once, because
/// the shed line named only the victim and never the cause (a stray
/// `rust-analyzer-proc-macro-srv` pair holding 140 GB on a 247 GiB box, against a
/// fleet holding 31 GB).
fn fleet_rss_bytes(slots: &[Slot]) -> (u64, usize) {
  slots
    .iter()
    .filter_map(|slot| slot.child.as_ref())
    .filter_map(|c| child_rss_bytes(c.id()))
    .fold((0, 0), |(sum, n), rss| (sum.saturating_add(rss), n + 1))
}

/// One memory-pressure governor step. Updates `shedding` with hysteresis (enter
/// below `floor`, exit above 1.5×`floor`) and, while shedding, SIGTERMs the
/// largest-RSS worker at most once per [`SHED_INTERVAL`]. It does not respawn or
/// reap — the sweep does both, and skips respawning while `*shedding`, so the
/// fleet shrinks under sustained pressure and refills once it clears.
///
/// `sheds` accumulates the run's total. A shed is not free: the victim's task is
/// re-leased, and since the victim is always the *largest-RSS* worker, the same
/// heavy paper tends to be re-picked on each retry until its retry budget is gone
/// and the dispatcher reports it `never_completed_with_retries`. That fatal is
/// then indistinguishable from a conversion bug, so the count is surfaced rather
/// than left to be inferred from the per-shed lines in a multi-GB log.
fn run_governor(
  floor: u64, slots: &[Slot], shedding: &mut bool, last_shed: &mut Option<Instant>, sheds: &mut u64
) {
  let Some(avail) = available_ram_bytes() else {
    return; // can't read memory → governor inactive this sweep
  };
  let ceiling = floor.saturating_add(floor / 2); // exit hysteresis at 1.5× floor
  if avail < floor {
    if !*shedding {
      // Name the cause, not just the symptom: the fleet can only ever free its
      // own RSS, so report that against everything else holding memory.
      let (fleet, live) = fleet_rss_bytes(slots);
      let foreign = total_ram_bytes()
        .unwrap_or(0)
        .saturating_sub(avail)
        .saturating_sub(fleet);
      warn!(
        target: "pericortex:harness",
        "memory pressure: MemAvailable {} MiB < floor {} MiB — shedding largest workers, pausing \
         respawns; this fleet holds {} MiB across {live} worker(s), {} MiB is held outside it",
        avail / MIB, floor / MIB, fleet / MIB, foreign / MIB
      );
      if fleet < foreign {
        warn!(
          target: "pericortex:harness",
          "most memory is held OUTSIDE this fleet — shedding conversions cannot free it; expect \
           re-leased tasks and spurious `never_completed_with_retries` fatals until the co-tenant \
           releases memory (check the box before reading these fatals as conversion regressions)"
        );
      }
      *shedding = true;
    }
    let now = Instant::now();
    let due = last_shed.is_none_or(|t| now.saturating_duration_since(t) >= SHED_INTERVAL);
    if due
      && let Some((idx, pid, rss)) = largest_rss_worker(slots)
    {
      *sheds += 1;
      warn!(
        target: "pericortex:harness",
        "shedding worker #{} (pid {pid}, RSS {} MiB) to relieve pressure; its task will be \
         re-leased (shed #{} this run)",
        idx + 1, rss / MIB, *sheds
      );
      if let Some(child) = slots[idx].child.as_ref() {
        term(child);
      }
      *last_shed = Some(now);
    }
  } else if avail > ceiling && *shedding {
    info!(
      target: "pericortex:harness",
      "memory pressure cleared: MemAvailable {} MiB > {} MiB — resuming respawns ({} shed(s) so \
       far this run)",
      avail / MIB, ceiling / MIB, *sheds
    );
    *shedding = false;
  }
}

/// Duration to sleep before the next sweep: the poll interval, shortened to the
/// soonest pending respawn deadline, or zero if any slot is ready to respawn
/// right now (so a ready slot is never made to wait out a full poll).
fn next_sleep(slots: &[Slot], poll: Duration) -> Duration {
  let now = Instant::now();
  let mut dur = poll;
  for slot in slots {
    if slot.child.is_some() {
      continue;
    }
    match slot.respawn_after {
      Some(t) if t > now => dur = dur.min(t - now),
      // Empty and eligible now (no pending backoff, or it already elapsed):
      // don't sleep — go straight back to the sweep to respawn it.
      _ => return Duration::ZERO,
    }
  }
  dur
}

/// Sleep up to `dur`, waking early on a shutdown signal *or* a SIGCHLD (a worker
/// died), so both Ctrl-C and respawns stay responsive between sweeps.
fn interruptible_sleep(dur: Duration) {
  let deadline = Instant::now() + dur;
  loop {
    if stop_requested() || CHILD_DIED.load(Ordering::SeqCst) {
      return;
    }
    let now = Instant::now();
    if now >= deadline {
      return;
    }
    thread::sleep(Duration::from_millis(200).min(deadline - now));
  }
}

/// Run the supervisor: maintain `config.workers` worker processes built by
/// `build` (called with a 1-based worker index, e.g. to label or shard them),
/// respawning any that exit — with crash-loop backoff — until SIGTERM/SIGINT,
/// then SIGTERM every worker and wait for them. `build` should produce a
/// **single-conversion** worker command (`--pool-size 1`); each child gets its
/// own unique ZMQ identity (the worker derives it from its PID), so the
/// dispatcher fans tasks out across the whole fleet.
///
/// Returns once every worker has been reaped after the shutdown signal. Errors
/// from individual spawns are logged and retried, not propagated — the fleet
/// degrades rather than aborting.
pub fn supervise<F>(config: &HarnessConfig, build: F) -> Result<(), Box<dyn Error>>
where
  F: Fn(usize) -> Command,
{
  install_signal_handlers();
  let n = config.workers.max(1);
  let now = Instant::now();
  let mut slots: Vec<Slot> = (1..=n)
    .map(|i| Slot {
      child: spawn_one(&build, i, config.mem_limit_bytes),
      spawned_at: now,
      fast_deaths: 0,
      respawn_after: None,
      last_cpu_ticks: None,
      cpu_advanced_at: now,
    })
    .collect();
  info!(target: "pericortex:harness", "supervising {n} single-conversion worker process(es)");
  if let Some(floor) = config.mem_pressure_floor_bytes {
    info!(
      target: "pericortex:harness",
      "memory-pressure governor active: shed largest worker below {} MiB MemAvailable",
      floor / MIB
    );
  }

  // Governor state (only touched when `mem_pressure_floor_bytes` is set).
  let mut shedding = false;
  let mut last_shed: Option<Instant> = None;
  let mut sheds: u64 = 0;

  while !stop_requested() {
    // Consume the wake hint up front; any death during this sweep/sleep re-arms
    // it, so we never miss one (and `try_wait` below is authoritative anyway).
    CHILD_DIED.store(false, Ordering::SeqCst);

    // Memory-pressure governor: decide (before respawning) whether we are under
    // pressure, and shed the largest worker if a shed is due.
    if let Some(floor) = config.mem_pressure_floor_bytes {
      run_governor(floor, &slots, &mut shedding, &mut last_shed, &mut sheds);
    }

    #[allow(clippy::needless_range_loop)]
    for slot in 0..n {
      if stop_requested() {
        break;
      }
      // 1. Reap a child that has exited (non-blocking). `Err` = unwaitable;
      //    treat as dead with an unknown status.
      let exited: Option<Option<ExitStatus>> = match slots[slot].child.as_mut() {
        Some(child) => match child.try_wait() {
          Ok(Some(status)) => Some(Some(status)),
          Ok(None) => None,    // still running
          Err(_) => Some(None) // unwaitable → dead, status unknown
        },
        None => None, // already empty (mid-backoff); nothing to reap
      };
      if let Some(status) = exited {
        if let Some(mut gone) = slots[slot].child.take() {
          let _ = gone.wait(); // reap the zombie
        }
        record_death(&mut slots[slot], slot + 1, status, config.respawn_backoff);
      }

      // 1b. Unresponsive-worker watchdog: a *live* worker whose CPU time has not
      //     advanced for `unresponsive_timeout` is wedged (blocked in a syscall,
      //     deadlocked, or a task whose own in-process watchdog never fired).
      //     Death-driven supervision can't see this — the process is still alive.
      //     SIGKILL it so the slot reaps + respawns on the next sweep; because it
      //     ran a long time, `record_death` reads it as a slow death (prompt
      //     respawn, no crash-loop backoff), and the dispatcher re-leases its
      //     in-flight task.
      if exited.is_none()
        && let Some(limit) = config.unresponsive_timeout
        && let Some(pid) = slots[slot].child.as_ref().map(|c| c.id())
        && let Some(cpu) = child_cpu_ticks(pid)
      {
        let now = Instant::now();
        if slots[slot].last_cpu_ticks != Some(cpu) {
          // Progress since last sweep (or first sample): reset the freeze clock.
          slots[slot].last_cpu_ticks = Some(cpu);
          slots[slot].cpu_advanced_at = now;
        } else if now.saturating_duration_since(slots[slot].cpu_advanced_at) >= limit {
          let stuck = now.saturating_duration_since(slots[slot].cpu_advanced_at);
          warn!(
            target: "pericortex:harness",
            "worker #{} (pid {pid}) unresponsive: no CPU progress for {:.0}s (>= {:.0}s watchdog) — SIGKILL + respawn; its task will be re-leased",
            slot + 1, stuck.as_secs_f64(), limit.as_secs_f64()
          );
          if let Some(child) = slots[slot].child.as_mut() {
            let _ = child.kill(); // SIGKILL: a wedged process may ignore SIGTERM
          }
          // Reset; the reap next sweep records a (slow) death and respawns.
          slots[slot].last_cpu_ticks = None;
          slots[slot].cpu_advanced_at = now;
        }
      }

      // 2. Respawn an empty slot once its backoff window has elapsed — unless we
      //    are shedding under memory pressure, in which case we let the fleet
      //    shrink until memory recovers.
      if !shedding && slots[slot].child.is_none() {
        let now = Instant::now();
        let eligible = slots[slot].respawn_after.is_none_or(|t| now >= t);
        if eligible && !stop_requested() {
          slots[slot].respawn_after = None;
          match spawn_one(&build, slot + 1, config.mem_limit_bytes) {
            Some(child) => {
              slots[slot].child = Some(child);
              slots[slot].spawned_at = now;
              // Fresh child: restart the CPU-progress watchdog from zero.
              slots[slot].last_cpu_ticks = None;
              slots[slot].cpu_advanced_at = now;
            },
            // Spawn failed (logged in spawn_one): wait one base backoff before
            // retrying so a persistent fork/exec failure can't spin.
            None => slots[slot].respawn_after = Some(now + config.respawn_backoff),
          }
        }
      }
    }

    // Sleep until the next event. While shedding, use a short fixed cadence so
    // we track a growing cluster and notice recovery quickly; otherwise sleep to
    // the soonest pending respawn / the poll backstop (woken early by SIGCHLD).
    let nap = if shedding {
      SHED_POLL
    } else {
      next_sleep(&slots, config.poll)
    };
    interruptible_sleep(nap);
  }

  // A run that shed is a run whose fatals need reading with care, so say so once
  // at the end rather than only in per-shed lines buried mid-log.
  if sheds > 0 {
    warn!(
      target: "pericortex:harness",
      "memory-pressure governor shed {sheds} worker(s) this run; each re-leased its task, so some \
       `never_completed_with_retries` fatals in this run may be shed victims rather than \
       conversion failures"
    );
  }

  // Graceful shutdown: SIGTERM every live worker, then wait for all to exit.
  info!(target: "pericortex:harness", "shutdown signal received — terminating workers");
  for slot in &slots {
    if let Some(child) = slot.child.as_ref() {
      term(child);
    }
  }
  for slot in &mut slots {
    if let Some(child) = slot.child.as_mut() {
      let _ = child.wait();
    }
  }
  info!(target: "pericortex:harness", "all workers stopped");
  Ok(())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn respawn_delay_is_exponential_and_capped() {
    let base = Duration::from_secs(1);
    // 0 or 1 fast deaths → base.
    assert_eq!(respawn_delay(base, 0), base);
    assert_eq!(respawn_delay(base, 1), base);
    // Then doubling.
    assert_eq!(respawn_delay(base, 2), Duration::from_secs(2));
    assert_eq!(respawn_delay(base, 3), Duration::from_secs(4));
    assert_eq!(respawn_delay(base, 4), Duration::from_secs(8));
    // Capped at MAX_RESPAWN_BACKOFF, never overflowing for large counts.
    assert_eq!(respawn_delay(base, 100), MAX_RESPAWN_BACKOFF);
    assert!(respawn_delay(base, u32::MAX) <= MAX_RESPAWN_BACKOFF);
  }

  #[test]
  fn workers_fitting_memory_no_cap_is_identity() {
    // No per-child cap → return the request unchanged (but never 0).
    assert_eq!(workers_fitting_memory(16, None), 16);
    assert_eq!(workers_fitting_memory(0, None), 1);
    assert_eq!(workers_fitting_memory(16, Some(0)), 16);
  }

  #[test]
  fn workers_fitting_memory_clamps_to_ram() {
    // Only meaningful where /proc/meminfo is readable (Linux CI/dev).
    let Some(total) = total_ram_bytes() else {
      return;
    };
    let usable = total / 100 * ram_budget_percent();
    // A per-child cap of half the usable budget admits ~2 workers, so a large
    // request must clamp down to that, and never below 1.
    let per_child = usable / 2;
    if per_child > 0 {
      let got = workers_fitting_memory(1024, Some(per_child));
      assert!((1..=3).contains(&got), "expected ~2 workers, got {got}");
    }
    // A tiny cap relative to RAM must not inflate beyond the request.
    assert_eq!(workers_fitting_memory(4, Some(1024 * 1024)), 4);
  }

  #[test]
  fn default_worker_count_is_at_least_one() {
    assert!(default_worker_count() >= 1);
  }

  #[test]
  fn fleet_rss_counts_live_workers_only() {
    // Nothing running → the fleet holds nothing, so all pressure is foreign.
    assert_eq!(fleet_rss_bytes(&[]), (0, 0));

    let Ok(child) = Command::new("sleep").arg("30").spawn() else {
      return; // no `sleep` on this host → nothing to measure
    };
    let now = Instant::now();
    let slot = |child| Slot {
      child,
      spawned_at: now,
      fast_deaths: 0,
      respawn_after: None,
      last_cpu_ticks: None,
      cpu_advanced_at: now
    };
    let mut slots = vec![slot(Some(child)), slot(None)];

    let (rss, live) = fleet_rss_bytes(&slots);
    // Only assertable where /proc/<pid>/status is readable (Linux dev/CI).
    if let Some(c) = slots[0].child.as_ref()
      && child_rss_bytes(c.id()).is_some()
    {
      assert_eq!(live, 1, "an empty slot must not be counted as a live worker");
      assert!(rss > 0, "a live worker must contribute its RSS");
    }

    if let Some(mut c) = slots[0].child.take() {
      let _ = c.kill();
      let _ = c.wait();
    }
  }

  #[test]
  fn parse_cpu_ticks_survives_comm_with_spaces_and_parens() {
    // comm = "(weird )na)me)" — embedded spaces AND parens, the pathological
    // case a naive `split_whitespace().nth(13)` gets wrong. Fields after the
    // closing paren: state ppid pgrp session tty tpgid flags minflt cminflt
    // majflt cmajflt utime(=111) stime(=222) ...
    let line = "12345 (weird )na)me) S 1 1 1 0 -1 0 10 0 0 0 111 222 5 6 20 0 1 0";
    assert_eq!(parse_cpu_ticks(line), Some(333));
    // A plain comm still parses.
    let plain = "42 (cortex_worker) R 1 1 1 0 -1 0 0 0 0 0 7 3 0 0 20 0 6 0";
    assert_eq!(parse_cpu_ticks(plain), Some(10));
    // Garbage / truncated → None, never a panic or a bogus tick count.
    assert_eq!(parse_cpu_ticks("no paren here"), None);
    assert_eq!(parse_cpu_ticks("1 (x) S 1 2 3"), None);
  }

  #[test]
  fn unresponsive_timeout_defaults_off() {
    // Generic consumers opt in; the check is dormant unless configured.
    assert!(HarnessConfig::default().unresponsive_timeout.is_none());
  }
}
