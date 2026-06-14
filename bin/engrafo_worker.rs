#![cfg(feature = "engrafo")]
extern crate num_cpus;
use pericortex::logger;
use pericortex::worker::{EngrafoWorker, Worker};

use std::env;
use std::error::Error;

// Sample runs:
// 1. Simple localhost test
// cortex run --bin engrafo_worker
// 2. 16 workers pointed at the live CorTeX endpoint
// cortex run --bin engrafo_worker 131.188.48.209 51695 51696 16

/// Start working on an Engrafo task for a given CorTeX endpoint
fn main() -> Result<(), Box<Error>> {
    // Info-level logging enabled.
    logger::init(log::LevelFilter::Info).unwrap();

    // Read input arguments, if any
    let mut input_args = env::args();
    let _ = input_args.next(); // skip process name
    let address = match input_args.next() {
        Some(address) => address,
        None => "131.188.48.209".to_string(),
    };
    let source_port = match input_args.next() {
        Some(port) => port.parse::<usize>().unwrap(),
        None => 51695,
    };
    let sink_port = match input_args.next() {
        Some(port) => port.parse::<usize>().unwrap(),
        None => 51696,
    };
    let pool_size = match input_args.next() {
        Some(count) => count.parse::<usize>().unwrap(),
        None => num_cpus::get(),
    };

    EngrafoWorker {
        service: "engrafo".to_string(),
        version: 2.0,
        message_size: 100_000,
        source: address.clone(),
        sink: address,
        source_port,
        sink_port,
        pool_size,
        ..EngrafoWorker::default()
    }
    .start(None)
}
