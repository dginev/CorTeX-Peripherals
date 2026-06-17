use super::Worker;
use std::borrow::Cow;
use std::env;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::process::Command;

/// A TeX to HTML conversion worker -- this is a demonstration only
/// it lacks robustness guards
/// see the Perl worker used in production for a full overviewthread::spawn(move || {
///  https://github.com/dginev/latexml-plugin-cortex
#[derive(Clone, Debug)]
pub struct TexToHtmlWorker {
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
    ///  the usual
    pub identity: String,
}
impl Default for TexToHtmlWorker {
    fn default() -> TexToHtmlWorker {
        TexToHtmlWorker {
            service: "tex_to_html".to_string(),
            version: 0.1,
            message_size: 100_000,
            source: "tcp://127.0.0.1:51695".to_string(),
            sink: "tcp://127.0.0.1:51696".to_string(),
            identity: String::new(),
        }
    }
}
impl Worker for TexToHtmlWorker {
    fn get_service(&self) -> &str {
        &self.service
    }
    fn get_source_address(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.source)
    }
    fn get_sink_address(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.sink)
    }
    fn message_size(&self) -> usize {
        self.message_size
    }
    fn get_identity(&self) -> &str {
        &self.identity
    }
    fn set_identity(&mut self, identity: String) {
        self.identity = identity;
    }

    fn convert(&self, path: &Path) -> Result<File, Box<dyn Error>> {
        let name = path.file_stem().unwrap().to_str().unwrap();
        let destination_path = env::temp_dir().to_str().unwrap().to_string() + "/" + name + ".zip";
        // println!("Source {:?}", path);
        Command::new("latexmlc")
            .arg("--whatsin")
            .arg("archive")
            .arg("--whatsout")
            .arg("archive")
            .arg("--format")
            .arg("html5")
            .arg("--pmml")
            .arg("--cmml")
            .arg("--mathtex")
            .arg("--preload")
            .arg("[ids]latexml.sty")
            .arg("--nodefaultresources")
            .arg("--inputencoding")
            .arg("iso-8859-1")
            .arg("--timeout")
            .arg("300")
            .arg("--log")
            .arg("cortex.log")
            .arg("--destination")
            .arg(destination_path.clone())
            .arg(path.to_string_lossy().to_string())
            .output()
            .unwrap_or_else(|e| panic!("failed to execute process: {}", e));

        // println!("Dest: {:?}", destination_path);
        File::open(destination_path).map_err(Into::into)
    }
}
