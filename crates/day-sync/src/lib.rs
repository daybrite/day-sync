// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Experimental plaintext replication over authenticated WebDAV/HTTPS.
//! The server and every authorized writer are trusted. Hashes are corruption checks, not signatures.
//! A versioned record boundary reserves room for a future separately reviewed protected format.
pub mod documents;
pub mod store;
pub use store::{Created, MemoryStore, Store, StoreError, StoreErrorKind, StoreFuture};

#[cfg(not(target_arch = "wasm32"))]
include!("replica.rs");
