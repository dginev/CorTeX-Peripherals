![CorTeX Peripherals](./public/img/logo.jpg) Peripherals
======

**Worker executables and the shared worker runtime for [CorTeX](https://github.com/dginev/CorTeX) — a general processing framework for scientific documents**

[![Build Status](https://github.com/dginev/CorTeX-Peripherals/workflows/CI/badge.svg)](https://github.com/dginev/CorTeX-Peripherals/actions?query=workflow%3ACI) [![License](https://img.shields.io/badge/license-CC0--1.0-blue.svg)](https://raw.githubusercontent.com/dginev/CorTeX-Peripherals/main/LICENSE) ![version](https://img.shields.io/badge/version-0.2.8-orange.svg)

The `pericortex::worker::Worker` trait automates a worker's ZeroMQ dialogue with a
CorTeX dispatcher: it requests a task for a named service from the ventilator,
streams the source archive to a scratch dir, runs the worker's `convert`, and
streams the result archive back to the sink — with bounded send/recv timeouts and
socket rebuild so a wedged/lost reply can never desync the request→reply pairing.
Implement `convert` (plus the service name and dispatcher addresses) and call
`start`; the loop is built so no transient fault (transport hiccup, full disk,
panicking conversion) takes the worker down.

Primary use case: latexml-oxide's `cortex_worker`
------

The runtime's main consumer today is latexml-oxide's `cortex_worker` — a
production-grade TeX→HTML worker that implements this `Worker` trait against the
in-process Rust LaTeXML engine, with per-paper panic/OOM/timeout isolation.

Because the converter runs in-process, the deployment model is **one conversion
per process**: each worker runs `--pool-size 1`, so a per-process RAM ceiling and
timeout bound exactly one document, and a timeout / OOM / panic / segfault takes
down only that worker — its single task is re-leased and the process respawned.
The `pericortex::harness` supervisor runs the fleet with memory-aware sizing,
prompt respawn of dead workers, and no orphaned converters left behind; see the
`harness` module docs for the mechanics.

Reference workers:

- `EchoWorker` — returns its input unchanged (round-trip / dispatcher testing).
- `TexToHtmlWorker` — TeX→HTML via a `latexmlc` subprocess (demonstration; lacks
  the production robustness guards of latexml-oxide's `cortex_worker`).
