//! `stitch`: a stable, persistent, in-order stream carried over a series of
//! `ssh` connections. See `doc/stitch.md` for the design and
//! `doc/implementation.md` for how the code is organized.

pub mod cli;
pub mod client;
pub mod core;
pub mod fdpass;
pub mod frame;
pub mod handshake;
pub mod paths;
pub mod runtime;
pub mod server;
pub mod sshconfig;
pub mod target;
