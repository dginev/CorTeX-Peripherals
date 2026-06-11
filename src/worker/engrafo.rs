#![cfg(feature = "engrafo")]
// Copyright 2015 Deyan Ginev. See the LICENSE
// file at the top-level directory of this distribution.
//
// Licensed under the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! a CorTeX worker for Engrafo, via a docker image

use std::borrow::Cow;
use std::env;
use std::fs::File;
use std::io::{Write};
use std::path::Path;
use std::process::Command;
use std::error::Error;
use tempfile::Builder;

use super::Worker;
use crate::adaptor;

/// An echo worker for testing
#[derive(Clone, Debug)]
pub struct EngrafoWorker {
  /// the usual
  pub service: String,
  /// the usual
  pub version: f32,
  /// the usual
  pub message_size: usize,
  /// the usual
  pub source: String,
  /// the usual
  pub sink: String,
  /// port to the source address
  pub source_port: usize,
  /// port to the sink address
  pub sink_port: usize,
  /// Allow for multiple parallel workers
  pub pool_size: usize,
  /// A uniquely identifying string, usually `hostname:engrafo:threadid`
  pub identity: String,
}
impl Default for EngrafoWorker {
  fn default() -> EngrafoWorker {
    EngrafoWorker {
      service: "engrafo".to_string(),
      version: 2.0,
      message_size: 100_000,
      source: "127.0.0.1".to_string(),
      source_port: 51695,
      sink: "127.0.0.1".to_string(),
      sink_port: 51696,
      pool_size: 1,
      identity: "unknown:engrafo:1".to_string(),
    }
  }
}

impl Worker for EngrafoWorker {
  fn get_service(&self) -> &str {
    &self.service
  }
  fn get_source_address(&self) -> Cow<str> {
    Cow::Owned(format!("tcp://{}:{}", self.source, self.source_port))
  }
  fn get_sink_address(&self) -> Cow<str> {
    Cow::Owned(format!("tcp://{}:{}", self.sink, self.sink_port))
  }
  fn message_size(&self) -> usize {
    self.message_size
  }
  fn pool_size(&self) -> usize {
    self.pool_size
  }
  fn set_identity(&mut self, identity: String) {
    self.identity = identity;
  }
  fn get_identity(&self) -> &str {
    &self.identity
  }

  fn convert(&self, path: &Path) -> Result<File, Box<Error>> {
    let input_tmpdir = adaptor::extract_zip_to_tmpdir(path, "engrafo_input")?;
    let unpacked_dir_path = input_tmpdir.path().to_str().unwrap().to_string() + "/";
    let destination_tmpdir = Builder::new().prefix("engrafo_output").tempdir().unwrap();
    let destination_dir_path = destination_tmpdir.path().to_str().unwrap();
    let tmp_dir_str = env::temp_dir().as_path().display().to_string();
    let docker_input_path = unpacked_dir_path.replace(&tmp_dir_str, "/workdir");
    let docker_output_path = destination_dir_path.replace(&tmp_dir_str, "/workdir");

    let cmd_result = Command::new("docker")
      .arg("run")
      .arg("-m")
      .arg("4g") // can be made customizeable based on architecture
      .arg("-v")
      .arg(format!("{}:/workdir", tmp_dir_str))
      .arg("-w")
      .arg("/workdir")
      .arg("arxivvanity/engrafo:2.0.0")
      .arg("engrafo")
      .arg(docker_input_path)
      .arg(docker_output_path)
      .output()
      .expect("failed to execute process engrafo docker process.");

    // Package the output -- cortex requires a single ZIP return,
    // with all logging information stored in a "cortex.log" file at the ZIP's root.

    let log_name = format!("{}/cortex.log", destination_dir_path);
    let cortex_log_path = Path::new(&log_name);
    {
      // write log file and close it before archiving.
      let mut log_file = File::create(&cortex_log_path)?;
      log_file.write_all(&cmd_result.stderr)?;
      log_file.write_all(&cmd_result.stdout)?;
    }

    // cleanup
    // By closing the `TempDir` explicitly, we can check that it has
    // been deleted successfully. If we don't close it explicitly,
    // the directory will still be deleted when `tmp_dir` goes out
    // of scope, but we won't know whether deleting the directory
    // succeeded.
    input_tmpdir.close().unwrap();

    adaptor::archive_tmpdir_to_zip(destination_tmpdir).map_err(Into::into)
  }
}
