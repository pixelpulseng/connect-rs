// Nonolith Connect — Rust port
// GPLv3+; original C++ (C) 2012 Nonolith Labs, LLC
//
// See SPEC.md in the C++ repository (../connect) for the behavioral
// specification this port implements.

pub mod device;
pub mod jsonutil;
pub mod listener;
pub mod net;
pub mod rest;
pub mod source;
pub mod streaming;
pub mod usb;
pub mod ws;

pub const SERVER_VERSION: &str = "1.3";

pub fn server_git_version() -> &'static str {
    option_env!("GITVERSION").unwrap_or("unknown")
}
