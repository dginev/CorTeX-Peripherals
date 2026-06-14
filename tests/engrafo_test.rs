#![cfg(feature = "engrafo")]
use std::io::Read;
use std::path::Path;

use pericortex::worker::{EngrafoWorker, Worker};

#[test]
fn unit_engrafo_test() {
    let worker = EngrafoWorker::default();
    // test we can convert a test doc
    let test_input_path = Path::new("tests/resources/1508.01222.zip");
    let converted = worker.convert(&test_input_path);
    assert!(converted.is_ok());
    let mut zip_file = converted.unwrap();
    let mut contents = vec![];
    assert!(zip_file.read_to_end(&mut contents).is_ok());
    assert!(contents.len() > 1_000_000); // make sure we have a reasonably sized ZIP, as a basic sanity check
}
