// Copyright 2015 Deyan Ginev. See the LICENSE
// file at the top-level directory of this distribution.
//
// Licensed under the MIT license <LICENSE-MIT or http://opensource.org/licenses/MIT>.
// This file may not be copied, modified, or distributed
// except according to those terms.

//! # The CorTeX library in Rust
//! The original library can be found at https://github.com/dginev/CorTeX

#![doc(html_root_url = "https://dginev.github.io/rust-cortex-peripherals/")]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/dginev/CorTeX/main/public/img/logo-icon.png"
)]
#![deny(missing_docs)]

#[macro_use]
extern crate log;

pub mod harness;
pub mod logger;
pub mod worker;
