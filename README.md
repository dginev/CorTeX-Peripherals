![CorTeX Peripherals](./public/img/logo.jpg) Peripherals
======

**Worker executables for [CorTeX](https://github.com/dginev/CorTeX) - a general processing framework for scientific documents**

[![Build Status](https://github.com/dginev/CorTeX-Peripherals/workflows/CI/badge.svg)](https://github.com/dginev/CorTeX-Peripherals/actions?query=workflow%3ACI) [![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/dginev/CorTeX-Peripherals/master/LICENSE) ![version](https://img.shields.io/badge/version-0.2.3-orange.svg)



1. [Engrafo](https://github.com/arxiv-vanity/engrafo) - tex-to-html conversion via latexml, with advanced styling and UX
  - uses a dedicated `docker` image which is an installation prerequisite.
  - builds under the `engrafo` feature flag, via `cargo test --features=engrafo`
  - starting a worker: `cargo run --release --features=engrafo --bin engrafo_worker`
