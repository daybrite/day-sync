// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! The remote store a space lives in: directories of immutable, content-addressed objects.
//!
//! The engine asks little of a store, and every transport can provide it: make a directory,
//! list one completely, read an object, write an object, write an object only while its name
//! is free, and delete one. Published objects are immutable and named by their content hash,
//! so an ambiguous outcome is settled by reading the object back; a store never has to be
//! transactional, keep versions, or lock anything. A transport crate implements [`Store`]
//! (`day-sync-webdav`, `day-sync-filesystem`); [`MemoryStore`] here serves tests.

use std::{
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

/// The future a store operation returns. Not `Send`: Day's task executor is single-threaded,
/// and some HTTP providers hand out futures that are not.
pub type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, StoreError>> + 'a>>;

/// The outcome of [`Store::create`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Created {
    /// The object was written under a name that was free.
    Created,
    /// An object already carried the name; nothing was written. Read it back to compare.
    Exists,
}

/// Result of publishing a local file. A backend that verified its remote staging file
/// before an acknowledged no-overwrite rename can return that digest to avoid a second GET.
#[derive(Debug)]
pub enum FileCreation {
    Created,
    Verified { sha256: [u8; 32], bytes: u64 },
    Exists,
}
impl From<FileCreation> for Created {
    fn from(file: FileCreation) -> Self {
        match file {
            FileCreation::Exists => Self::Exists,
            FileCreation::Created | FileCreation::Verified { .. } => Self::Created,
        }
    }
}
impl From<Created> for FileCreation {
    fn from(outcome: Created) -> Self {
        match outcome {
            Created::Created => Self::Created,
            Created::Exists => Self::Exists,
        }
    }
}

/// A content read, optionally reusing a previously verified strong validator at this exact URL.
pub enum ObjectRead {
    Bytes(Vec<u8>, Option<String>),
    Unchanged,
    Absent,
}

/// Where a space's objects live. `directory` is a path of segments below the store's root;
/// `name` is one object inside it. Segments and names never contain path separators.
///
/// Implementations may block, but must be cancellation-safe: a dropped future leaves the store
/// in a state the next call can reconcile, which immutable content-addressed objects guarantee
/// as long as a partially written object is never visible under its final name.
pub trait Store: Send + Sync {
    /// What this store stands for, pinned into the database on the first sync so a replica
    /// never mixes two stores: a `WebDAV` endpoint URL, a directory's canonical path.
    fn endpoint(&self) -> String;
    /// Make `directory` and its parents exist. An existing directory is success.
    fn ensure_directory<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, ()>;
    /// The names of everything directly inside `directory`, complete or an error: a listing
    /// the store cannot vouch for fails with [`StoreErrorKind::IncompleteListing`], never
    /// returns a subset. A missing directory is [`StoreErrorKind::NotFound`].
    fn list<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, Vec<String>>;
    /// The object's bytes, or `None` when no object carries the name.
    fn get<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
    ) -> StoreFuture<'a, Option<Vec<u8>>>;
    /// Conditional asset read. Stores without strong validators use a full verified read.
    fn read_object<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        _validator: Option<&'a str>,
    ) -> StoreFuture<'a, ObjectRead> {
        Box::pin(async move {
            Ok(self
                .get(directory, name)
                .await?
                .map_or(ObjectRead::Absent, |bytes| ObjectRead::Bytes(bytes, None)))
        })
    }
    /// Write the object, replacing any object of that name.
    fn put<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, ()>;
    /// Write the object only if no object carries the name yet.
    fn create<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, Created>;
    /// Publish a whole file without replacing an existing object. The default supports small
    /// files on buffered stores. Disk and DAV stores support large files through their file-transfer APIs.
    fn create_file<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        file: &'a std::path::Path,
        limit: u64,
    ) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            use std::io::Read;
            let limit = limit.min(8 * 1024 * 1024);
            let mut data = Vec::new();
            std::fs::File::open(file)
                .and_then(|f| f.take(limit + 1).read_to_end(&mut data))
                .map_err(StoreError::local_io)?;
            if data.len() as u64 > limit {
                return Err(StoreError::new(StoreErrorKind::Limit));
            }
            self.create(directory, name, data).await
        })
    }
    /// Create a file with cumulative transfer progress on the polling task. Backends
    /// without intermediate events report the completed size after a successful write.
    fn create_file_with_progress<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        file: &'a std::path::Path,
        limit: u64,
        progress: &'a mut dyn FnMut(u64),
    ) -> StoreFuture<'a, FileCreation> {
        Box::pin(async move {
            let size = file.metadata().map_err(StoreError::local_io)?.len();
            let outcome = self.create_file(directory, name, file, limit).await?;
            if outcome == Created::Created {
                progress(size);
            }
            Ok(outcome.into())
        })
    }
    /// Write at most `limit` bytes to the caller's staging sink. Returns false for a missing file.
    fn read_file<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        sink: &'a mut dyn std::io::Write,
        limit: u64,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let Some(data) = self.get(directory, name).await? else {
                return Ok(false);
            };
            if data.len() as u64 > limit {
                return Err(StoreError::new(StoreErrorKind::Limit));
            }
            sink.write_all(&data).map_err(StoreError::local_io)?;
            Ok(true)
        })
    }
    /// Remove the object. `Ok(true)` when it existed, `Ok(false)` when there was nothing to
    /// remove; absence is never an error.
    fn delete<'a>(&'a self, directory: &'a [String], name: &'a str) -> StoreFuture<'a, bool>;
}

/// Why a store operation failed, in the terms the engine and its callers act on. The
/// transport's own error travels along as the source for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreErrorKind {
    /// The store refused the credentials; a person has to act (HTTP 401).
    Unauthorized,
    /// The store refused the operation itself: a forbidden path, a read-only mount (HTTP 403).
    Forbidden,
    /// The store is out of space or quota (HTTP 507, `ENOSPC`).
    InsufficientStorage,
    /// The store or the network is unavailable right now; retrying later is reasonable
    /// (connection failures, timeouts, HTTP 423, 429 and 5xx).
    Unavailable,
    /// A response exceeded a bound this store enforces.
    Limit,
    /// The endpoint, directory or name is not acceptable to this store.
    Invalid,
    /// The store answered a listing it could not vouch for as complete.
    IncompleteListing,
    /// The directory or object does not exist, where absence is an error.
    NotFound,
    /// The store answered in a way this implementation does not understand.
    Protocol,
    /// Anything else; see the source.
    Other,
}

/// A failed store operation.
#[derive(Debug)]
pub struct StoreError {
    pub kind: StoreErrorKind,
    /// A minimum wait the store asked for before the next attempt.
    pub retry_after: Option<Duration>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}
impl StoreError {
    /// Classify local staging and destination I/O independently of the remote transport.
    pub fn local_io(error: std::io::Error) -> Self {
        let kind = match error.kind() {
            std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded => {
                StoreErrorKind::InsufficientStorage
            }
            _ => StoreErrorKind::Other,
        };
        Self::with_source(kind, error)
    }
    pub fn new(kind: StoreErrorKind) -> Self {
        Self {
            kind,
            retry_after: None,
            source: None,
        }
    }
    pub fn with_source(
        kind: StoreErrorKind,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            retry_after: None,
            source: Some(Box::new(source)),
        }
    }
    #[must_use]
    pub fn retry_after(mut self, wait: Option<Duration>) -> Self {
        self.retry_after = wait;
        self
    }
}
impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.kind {
            StoreErrorKind::Unauthorized => "store.unauthorized",
            StoreErrorKind::Forbidden => "store.forbidden",
            StoreErrorKind::InsufficientStorage => "store.insufficient_storage",
            StoreErrorKind::Unavailable => "store.unavailable",
            StoreErrorKind::Limit => "store.limit",
            StoreErrorKind::Invalid => "store.invalid",
            StoreErrorKind::IncompleteListing => "store.incomplete_listing",
            StoreErrorKind::NotFound => "store.not_found",
            StoreErrorKind::Protocol => "store.protocol",
            StoreErrorKind::Other => "store.other",
        })
    }
}
impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

type Directories = BTreeMap<Vec<String>, BTreeMap<String, Vec<u8>>>;

/// A store that lives in this process. For tests, and for one app to talk to itself.
#[derive(Clone)]
pub struct MemoryStore {
    id: String,
    inner: Arc<Mutex<Directories>>,
}
impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}
impl MemoryStore {
    pub fn new() -> Self {
        Self {
            #[cfg(not(target_arch = "wasm32"))]
            id: uuid::Uuid::new_v4().to_string(),
            #[cfg(target_arch = "wasm32")]
            id: {
                static NEXT: std::sync::atomic::AtomicUsize =
                    std::sync::atomic::AtomicUsize::new(0);
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    .to_string()
            },
            inner: Arc::default(),
        }
    }
    fn directories(&self) -> std::sync::MutexGuard<'_, Directories> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// A copy of one directory's objects, for assertions.
    pub fn objects(&self, directory: &[String]) -> BTreeMap<String, Vec<u8>> {
        self.directories()
            .get(directory)
            .cloned()
            .unwrap_or_default()
    }
    /// Remove an object behind the engine's back, as a server losing a file would.
    pub fn lose(&self, directory: &[String], name: &str) -> bool {
        self.directories()
            .get_mut(directory)
            .is_some_and(|d| d.remove(name).is_some())
    }
    /// Overwrite an object's bytes behind the engine's back, as a corrupting server would.
    pub fn corrupt(&self, directory: &[String], name: &str, bytes: Vec<u8>) -> bool {
        self.directories()
            .get_mut(directory)
            .is_some_and(|d| d.insert(name.to_owned(), bytes).is_some())
    }
}
impl Store for MemoryStore {
    fn endpoint(&self) -> String {
        format!("memory:{}", self.id)
    }
    fn ensure_directory<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mut directories = self.directories();
            for n in 1..=directory.len() {
                directories.entry(directory[..n].to_vec()).or_default();
            }
            Ok(())
        })
    }
    fn list<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        Box::pin(async move {
            let directories = self.directories();
            let objects = directories
                .get(directory)
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?;
            let children = directories
                .keys()
                .filter(|k| k.len() == directory.len() + 1 && k.starts_with(directory))
                .map(|k| k[directory.len()].clone());
            Ok(objects.keys().cloned().chain(children).collect())
        })
    }
    fn get<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            Ok(self
                .directories()
                .get(directory)
                .and_then(|d| d.get(name).cloned()))
        })
    }
    fn put<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.directories()
                .get_mut(directory)
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?
                .insert(name.to_owned(), bytes);
            Ok(())
        })
    }
    fn create<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            let mut directories = self.directories();
            let objects = directories
                .get_mut(directory)
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?;
            if objects.contains_key(name) {
                return Ok(Created::Exists);
            }
            objects.insert(name.to_owned(), bytes);
            Ok(Created::Created)
        })
    }
    fn delete<'a>(&'a self, directory: &'a [String], name: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            Ok(self
                .directories()
                .get_mut(directory)
                .is_some_and(|d| d.remove(name).is_some()))
        })
    }
}
