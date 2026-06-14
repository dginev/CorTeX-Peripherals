//! This module is a trimmed-down copy of rtx_core::util::logger,
//! which is still waiting to get released as a crate...
//! maybe there is a simple logger crate that achieves this exact behavior?
use chrono::Local;
use log::max_level;
use log::{Level, LevelFilter, Metadata, Record, SetLoggerError};

struct RtxLogger;
static LOGGER: RtxLogger = RtxLogger;

/// Wrap `text` in an ANSI SGR color escape; an empty `code` leaves it
/// unstyled (matching the old `ansi_term` `Style::default()`). Replaces the
/// unmaintained `ansi_term` crate (RUSTSEC-2021-0139) with a zero-dependency
/// hand-rolled painter.
fn paint(code: &str, text: &str) -> String {
    if code.is_empty() {
        text.to_string()
    } else {
        format!("\x1b[{}m{}\x1b[0m", code, text)
    }
}

/// Convenient printing to STDERR (with \n)
#[macro_export]
macro_rules! println_stderr(
    ($($arg:tt)*) => ({
      use std::io::Write;
      match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
        Ok(_) => {},
        Err(x) => panic!("Unable to write to stderr: {}", x),
      }
    })
);

/// Convenient printing to STDERR
#[macro_export]
macro_rules! print_stderr(
    ($($arg:tt)*) => ({
      use std::io::Write;
      match write!(&mut ::std::io::stderr(), $($arg)* ) {
        Ok(_) => {},
        Err(x) => panic!("Unable to write to stderr: {}", x),
      }
    })
);

impl log::Log for RtxLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let record_target = record.target();
            let details = record.args();
            let category_object = if record_target.is_empty() {
                "" // "unknown:unknown" ???
            } else {
                record_target
            };
            // Following the reporting syntax at: http://dlmf.nist.gov/LaTeXML/manual/errorcodes/
            // let severity = if category_object.starts_with("Fatal:") {
            //   ""
            // } else {
            //   match record.level() {
            //     Level::Info => "Info",
            //     Level::Warn => "Warn",
            //     Level::Error => "Error",
            //     Level::Debug => "Debug",
            //     Level::Trace => "Trace",
            //   }
            // };

            let message = format!("{}\t", category_object);

            let color_code = match record.level() {
                Level::Info => "32",  // Green
                Level::Warn => "33",  // Yellow
                Level::Error => "31", // Red
                Level::Debug => "",   // default (no color)
                _ => "37",            // White
            };
            let painted_message = paint(color_code, &message) + &details.to_string();

            println_stderr!(
                "\r[{}] {}",
                Local::now().format("%Y-%m-%d %H:%M:%S"),
                painted_message
            );
        }
    }

    fn flush(&self) {}
}

/// Initialize the logger with an appropriate level of verbosity
pub fn init(level: LevelFilter) -> Result<(), SetLoggerError> {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(level);
    Ok(())
}
