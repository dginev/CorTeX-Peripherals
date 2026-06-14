use super::Worker;
use std::borrow::Cow;
use std::error::Error;
use std::fs::File;
use std::path::Path;

/// An echo worker for testing
#[derive(Clone, Debug)]
pub struct EchoWorker {
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
    /// the usual
    pub identity: String,
}
impl Default for EchoWorker {
    fn default() -> EchoWorker {
        EchoWorker {
            service: "echo_service".to_string(),
            version: 0.1,
            message_size: 100_000,
            source: "tcp://127.0.0.1:51695".to_string(),
            sink: "tcp://127.0.0.1:51696".to_string(),
            identity: "echo worker".to_string(),
        }
    }
}
impl Worker for EchoWorker {
    fn get_service(&self) -> &str {
        &self.service
    }
    fn get_source_address(&self) -> Cow<str> {
        Cow::Borrowed(&self.source)
    }
    fn get_sink_address(&self) -> Cow<str> {
        Cow::Borrowed(&self.sink)
    }
    fn message_size(&self) -> usize {
        self.message_size
    }

    fn convert(&self, path: &Path) -> Result<File, Box<dyn Error>> {
        File::open(path).map_err(Into::into)
    }
    fn set_identity(&mut self, identity: String) {
        self.identity = identity;
    }
    fn get_identity(&self) -> &str {
        &self.identity
    }
}
