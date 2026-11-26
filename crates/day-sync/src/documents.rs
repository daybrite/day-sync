// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Exchange independently mergeable documents with multiple stores, including from a browser.
//! Each application owns its document schema, causal merge and durable local transactions.
//! A missing remote object never means deletion. Applications represent deletions in the data.

use crate::{Store, StoreError, StoreErrorKind, StoreFuture};

/// Bytes and an opaque, strong validator for exactly those bytes.
pub struct Version {
    pub bytes: Vec<u8>,
    pub validator: String,
}

/// Atomic create and conditional replacement. A false result means another writer won.
pub trait DocumentStore: Store {
    /// Filenames with strong validators when the server exposes them in its listing.
    fn index<'a>(
        &'a self,
        directory: &'a [String],
    ) -> StoreFuture<'a, Vec<(String, Option<String>)>> {
        Box::pin(async move {
            Ok(self
                .list(directory)
                .await?
                .into_iter()
                .map(|n| (n, None))
                .collect())
        })
    }

    fn version<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
    ) -> StoreFuture<'a, Option<Version>>;
    fn replace<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        expected: Option<&'a str>,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, bool>;
}

/// Merge and verify one document. The callback must validate its input, be deterministic
/// and never drop causally independent state. The caller merges the returned bytes into
/// *current* local state, since local edits may have occurred during the exchange.
///
/// # Errors
/// Returns transport or validation errors. Repeated contention is retryable. An ambiguous
/// write is read back before deciding whether another attempt is necessary.
pub async fn reconcile(
    store: &dyn DocumentStore,
    directory: &[String],
    name: &str,
    mut merge: impl FnMut(Option<&[u8]>) -> Result<Vec<u8>, StoreError>,
) -> Result<Version, StoreError> {
    for _ in 0..8 {
        let current = store.version(directory, name).await?;
        let bytes = merge(current.as_ref().map(|v| v.bytes.as_slice()))?;
        if let Some(v) = current.as_ref().filter(|v| v.bytes == bytes) {
            return Ok(Version {
                bytes,
                validator: v.validator.clone(),
            });
        }
        let written = store
            .replace(
                directory,
                name,
                current.as_ref().map(|v| v.validator.as_str()),
                bytes.clone(),
            )
            .await;
        if matches!(written, Ok(false)) {
            continue;
        }
        match store.version(directory, name).await {
            Ok(Some(v)) if v.bytes == bytes => return Ok(v),
            _ => {
                written?;
            }
        }
    }
    Err(StoreError::new(StoreErrorKind::Unavailable))
}
