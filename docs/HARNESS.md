# The process-supervising harness (`pericortex::harness`)

The shared **mechanism** for running a fleet of CorTeX workers as a supervised
set of **single-conversion child processes**, with the resource discipline a
long-running fleet on adversarial input needs. This document covers the harness
library only; for how latexml-oxide *drives* it see that repo's
`docs/CORTEX_WORKER_HARNESS.md`, and for operator deployment see CorTeX's
`MANUAL.md` §7.

## Why one conversion per process

An in-process converter that digests adversarial documents *will* occasionally
run away — unbounded macro expansion, a pathological box accumulation, a panic,
a native segfault, or a wall-clock hang. The robust containment for that is the
OS process boundary:

* **One conversion per process** (`Worker::pool_size() == 1`) means each paper
  gets its own RAM ceiling and its own wall-clock budget. A timeout / OOM /
  panic / segfault kills exactly **one** worker — never the fleet.
* The dispatcher's lease reaper re-leases the single task that worker was
  holding, and the harness **respawns** a fresh process in its place.

Running one process with `--pool-size N` instead would share a single RAM
ceiling across N concurrent conversions, so the process RSS blows the cap and
the memory guards false-positive on *every* in-flight paper. That model is
intentionally avoided here.

This is the Rust successor to the historical Perl
`LaTeXML-Plugin-Cortex/bin/latexml_harness`, extended with the properties a
many-core fleet needs at scale: memory-aware behaviour, crash-loop containment,
prompt respawn, and no orphaned processes.

## What `supervise` does

```rust
pub fn supervise<F>(config: &HarnessConfig, build: F) -> Result<(), Box<dyn Error>>
where F: Fn(usize) -> Command;
```

* Spawns `config.workers` child processes, each from a fresh `Command` produced
  by `build(index)` (the 1-based index lets the caller label/shard them). Each
  child, before it execs, gets `PR_SET_PDEATHSIG` and (if set) its `RLIMIT_AS`
  cap — see [The per-child setup](#the-per-child-setup-pre_exec).
* **Death detection is SIGCHLD-driven**: a worker that exits wakes the
  supervision loop within ~200 ms rather than after the full `config.poll`
  interval. `config.poll` remains the periodic backstop, and `Child::try_wait`
  is the authoritative reaper, so a missed/spurious signal only affects latency,
  never correctness.
* **Crash-loop containment**: a worker that dies *fast* (before
  `FAST_DEATH_THRESHOLD`, 5 s) *and uncleanly* (non-zero / signal exit) is
  treated as a crash loop and respawned with **exponential backoff** —
  `respawn_backoff`, doubling per consecutive fast death, capped at
  `MAX_RESPAWN_BACKOFF` (60 s). A **clean** exit (a completed `--limit`) or a
  **slow** death (a real per-paper OOM/timeout/panic *after* startup) resets the
  backoff and respawns promptly. Backoff is non-blocking (a per-slot
  `respawn_after` deadline), so one wedged slot never delays the others.
* Installs a SIGTERM/SIGINT handler; on signal it stops leasing, SIGTERMs every
  live child (graceful — lets their sockets close on exit), waits for all to
  exit, and returns. A spawn error is logged and retried on the next sweep,
  never propagated — the fleet degrades rather than aborting.

`build` should produce a **single-conversion** command (`--pool-size 1`). Each
child derives its own globally-unique ZMQ identity from its PID, so the
dispatcher fans tasks across the whole fleet.

`HarnessConfig::default()` uses `default_worker_count()` — the CPU count minus a
reservation for the OS + dispatcher (subtract 4 above 16 cores, 2 above 4, 1
above 2; never returns 0).

## The two memory limits, and why they're different

A fleet should **over-commit deliberately**. In practice most conversions use a
small fraction of any reasonable per-job ceiling (a few hundred MB), while a
rare *legitimate* paper needs several GB. Sizing the fleet to the worst case
(`workers × cap ≤ RAM`) would idle most of a big box for a scenario that almost
never happens. So the harness runs the full CPU-derived worker count and bounds
memory with **two complementary limits**:

### 1. Per-child `RLIMIT_AS` — contains a *single* job (`HarnessConfig::mem_limit_bytes`)

When set, the harness applies `setrlimit(RLIMIT_AS, bytes)` inside each forked
child **before `exec`**. When a capped child later allocates past the ceiling,
the allocation fails with `ENOMEM`; a worker built around an alloc-error hook
(latexml-oxide's `cortex_worker` is) turns that into a clean `Fatal:oom` + a
non-zero exit, and the supervisor respawns it — so one runaway costs **exactly
one paper, attributably**, never the fleet, and never a silent kernel OOM-kill.

`RLIMIT_AS` is the **only** `setrlimit` knob that constrains memory portably
without privileges:

| Knob | Caps | Verdict |
| --- | --- | --- |
| `RLIMIT_RSS` | resident RAM | **no-op on Linux** (ignored since 2.6) |
| `RLIMIT_AS` | virtual address space (VSZ) | what we use — privilege-free, `pre_exec`-settable |
| `RLIMIT_DATA` | brk + mmap (Linux ≥4.7) | same VSZ over-count problem as `RLIMIT_AS` |
| cgroup `memory.max` | **physical RAM** | true RSS cap, but **external** (needs cgroup delegation / a container / systemd), and a breach is a silent SIGKILL |

So `mem_limit_bytes` bounds **address space (VSZ)**, not resident RAM (RSS).
With an arena allocator such as **mimalloc** (which latexml-oxide's worker uses
to avoid glibc arena-mutex contention), VSZ runs above RSS, so the *real*
resident kill point sits **below** the configured number. Treat the cap as a
conservative bound and size it at or above the intended per-job RSS ceiling.

### 2. The fleet memory-pressure governor — contains the *aggregate* (`HarnessConfig::mem_pressure_floor_bytes`)

The per-child cap does nothing about the *aggregate*: a cluster of
simultaneously-heavy (even legitimate) jobs, each under its own cap, can still
sum past physical RAM. The governor is the in-process answer:

* Each sweep it reads system `MemAvailable` (the kernel's own allocatable-without-
  swapping estimate — it accounts for reclaimable page cache and *all* memory
  users, not just worker RSS).
* While `MemAvailable` is **below the floor**, it SIGTERMs the **largest-RSS**
  worker (freeing the most memory per kill minimises total kills; the
  dispatcher re-leases that one paper) and **pauses respawns**, so the fleet
  shrinks under sustained pressure. It sheds at most one worker per
  `SHED_INTERVAL` (3 s) so each kill can settle before the next decision, and
  switches to a short `SHED_POLL` (1 s) cadence to track a fast-growing cluster.
* Once `MemAvailable` recovers **past 1.5× the floor** (hysteresis), it resumes
  respawning and the fleet refills.

This makes over-commit safe: the common case runs the full worker count and the
governor never fires; only a genuine heavy cluster triggers graceful,
attributable shedding instead of an uncontrolled kernel OOM-kill — which would
pick a victim at random, possibly the dispatcher or the harness itself.

`mem_pressure_floor_bytes = None` disables the governor. For a *hard* backstop,
layer a cgroup `memory.max` *outside* the harness (a memory-limited container or
`systemd-run --scope -p MemoryMax=…`); the three compose — the cgroup caps the
host aggregate, the governor sheds proactively before that cap, and `RLIMIT_AS`
caps each individual child.

`workers_fitting_memory(requested, mem_limit_bytes)` is provided for the
*opposite* policy — callers who want a hard guarantee of **no** over-commit get
`min(requested, usable_RAM / mem_limit)` (headroom via
`CORTEX_HARNESS_RAM_HEADROOM_PCT`, default 85%). It is not applied by default.

## The per-child setup (`pre_exec`)

Each spawned child runs a small hook in the forked process **before `exec`**
(only async-signal-safe syscalls — `prctl`/`getrlimit`/`setrlimit`, no heap, no
locks):

* **`PR_SET_PDEATHSIG(SIGTERM)`** (Linux) — if the harness is SIGKILLed or
  crashes, the kernel SIGTERMs its converting children rather than leaving them
  orphaned (unsupervised processes that keep consuming CPU/RAM/sockets). Sound
  here because the harness's spawning thread runs `supervise` for the whole
  process lifetime, so the documented "fires when the spawning *thread* exits"
  gotcha can't trigger early.
* **`RLIMIT_AS`** (when `mem_limit_bytes` is set) — only ever **lowered**, never
  raised above an ambient hard limit (raising needs privilege), so it composes
  safely under an already-tight `ulimit -v`.

## Minimal usage

```rust
use std::process::Command;
use pericortex::harness::{supervise, HarnessConfig, default_worker_count, total_ram_bytes};

let exe = std::env::current_exe()?;
let cap = 8 * 1024 * 1024 * 1024; // 8 GiB per-child address-space cap
let config = HarnessConfig {
    workers: default_worker_count(),                 // deliberate over-commit
    mem_limit_bytes: Some(cap),                       // contains one runaway job
    // governor floor: shed when free RAM drops below 10% (contains the aggregate)
    mem_pressure_floor_bytes: total_ram_bytes().map(|t| t / 10),
    ..Default::default()
};
supervise(&config, move |_index| {
    let mut cmd = Command::new(&exe);
    cmd.arg("--pool-size").arg("1")
       .arg("--source-address").arg("tcp://dispatcher:51695")
       .arg("--sink-address").arg("tcp://dispatcher:51696");
    cmd
})?;
```

The build closure must be `Fn` (it is called once per spawn, including every
respawn) and must **not** re-add the harness flag to the child, or the fleet
would fork-bomb.
