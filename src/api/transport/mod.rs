//! Transport adapters — currently: multiplexing one transport across many
//! Raft groups.

pub mod multiplex;

pub use multiplex::MultiplexedTransport;
