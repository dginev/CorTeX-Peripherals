![CorTeX Peripherals](./public/img/logo.jpg) Peripherals
======

**Worker executables and the shared worker runtime for [CorTeX](https://github.com/dginev/CorTeX) — a general processing framework for scientific documents**

[![Build Status](https://github.com/dginev/CorTeX-Peripherals/workflows/CI/badge.svg)](https://github.com/dginev/CorTeX-Peripherals/actions?query=workflow%3ACI) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/dginev/CorTeX-Peripherals/main/LICENSE) ![version](https://img.shields.io/badge/version-0.2.6-orange.svg)

The `pericortex::worker::Worker` trait automates a worker's ZeroMQ dialogue with a
CorTeX dispatcher: it requests a task for a named service from the ventilator,
streams the source archive to a scratch dir, runs the worker's `convert`, and
streams the result archive back to the sink — with bounded send/recv timeouts and
socket rebuild so a wedged/lost reply can never desync the request→reply pairing.
Implement `convert` (plus the service name and dispatcher addresses) and call
`start`; the loop is built so no transient fault (transport hiccup, full disk,
panicking conversion) takes the worker down.

Reference workers:

- `EchoWorker` — returns its input unchanged (round-trip / dispatcher testing).
- `TexToHtmlWorker` — TeX→HTML via a `latexmlc` subprocess (demonstration; lacks
  production robustness guards).

For the production-grade TeX→HTML worker (per-paper panic/OOM/timeout isolation,
mimalloc, release tuning) see latexml-oxide's `cortex_worker` binary, which
implements this same `Worker` trait against the in-process Rust engine.
