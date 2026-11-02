// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A directory as a [`day_sync::Store`].
//!
//! The directory can be local, which lets two apps on one machine synchronize with no server,
//! a mounted network share, or a folder another program carries between machines (iCloud
//! Drive, Dropbox, Syncthing). Objects are written to a hidden temporary file in the same
//! directory and renamed into place, so a reader never sees a partially written object under
//! its final name. Create-only writes link the temporary file to the final name, which fails
//! atomically when the name is taken; a file system without hard links falls back to a check
//! and a rename, the one non-atomic step, and the engine's read-back covers it.
//!
//! Calls block on `std::fs`; they are short, and Day's executor runs them on the task's
//! thread. Names beginning with a dot are reserved for this store's temporary files and are
//! neither listed nor accepted.
#![cfg(not(target_arch = "wasm32"))]

pub use day_sync;

use day_sync::{Created, Store, StoreError, StoreErrorKind, StoreFuture};
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// A directory serving as a store. Its canonical path is the endpoint a replica pins.
#[derive(Clone, Debug)]
pub struct FilesystemStore {
    root: PathBuf,
    endpoint: String,
}
impl FilesystemStore {
    /// Use `root` as the store, creating it if it does not exist.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        fs::create_dir_all(root.as_ref()).map_err(convert)?;
        let root = root.as_ref().canonicalize().map_err(convert)?;
        let endpoint = format!("file://{}", root.display());
        Ok(Self { root, endpoint })
    }
    pub fn root(&self) -> &Path {
        &self.root
    }
    fn resolve(&self, directory: &[String], name: Option<&str>) -> Result<PathBuf, StoreError> {
        let mut path = self.root.clone();
        for segment in directory.iter().map(String::as_str).chain(name) {
            if !valid_name(segment) {
                return Err(StoreError::new(StoreErrorKind::Invalid));
            }
            path.push(segment);
        }
        Ok(path)
    }
}

/// One path component the store accepts: non-empty, no separators or NUL, not hidden, and
/// short enough for every common file system.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('.')
        && !name.chars().any(|c| matches!(c, '/' | '\\' | '\0' | ':'))
}

fn convert(error: io::Error) -> StoreError {
    use StoreErrorKind as K;
    use io::ErrorKind as E;
    let kind = match error.kind() {
        E::NotFound => K::NotFound,
        E::PermissionDenied | E::ReadOnlyFilesystem => K::Forbidden,
        E::StorageFull | E::QuotaExceeded => K::InsufficientStorage,
        E::Interrupted
        | E::TimedOut
        | E::WouldBlock
        | E::ResourceBusy
        | E::HostUnreachable
        | E::NetworkUnreachable
        | E::NetworkDown
        | E::StaleNetworkFileHandle
        | E::ConnectionReset
        | E::ConnectionAborted
        | E::BrokenPipe => K::Unavailable,
        E::InvalidInput | E::InvalidData => K::Invalid,
        _ => K::Other,
    };
    StoreError::with_source(kind, error)
}

static TEMPORARY: AtomicU64 = AtomicU64::new(0);

/// Write `bytes` to a hidden temporary file beside `path`, flushed to disk.
fn stage(path: &Path, bytes: &[u8]) -> Result<PathBuf, StoreError> {
    let directory = path
        .parent()
        .ok_or_else(|| StoreError::new(StoreErrorKind::Invalid))?;
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| StoreError::new(StoreErrorKind::Invalid))?;
    let temporary = directory.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        TEMPORARY.fetch_add(1, Ordering::Relaxed)
    ));
    let written = (|| {
        use io::Write;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()
    })();
    if let Err(e) = written {
        let _ = fs::remove_file(&temporary);
        return Err(convert(e));
    }
    Ok(temporary)
}

/// Make the directory entry durable where the platform offers it; a failure here is a
/// weaker guarantee, not a failed write.
fn sync_directory(path: &Path) {
    #[cfg(unix)]
    if let Some(directory) = path.parent()
        && let Ok(file) = fs::File::open(directory)
    {
        let _ = file.sync_all();
    }
    #[cfg(not(unix))]
    let _ = path;
}

impl Store for FilesystemStore {
    fn endpoint(&self) -> String {
        self.endpoint.clone()
    }
    fn ensure_directory<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, ()> {
        Box::pin(async move { fs::create_dir_all(self.resolve(directory, None)?).map_err(convert) })
    }
    fn list<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        Box::pin(async move {
            let mut names = vec![];
            for entry in fs::read_dir(self.resolve(directory, None)?).map_err(convert)? {
                let entry = entry.map_err(convert)?;
                if let Some(name) = entry.file_name().to_str()
                    && !name.starts_with('.')
                {
                    names.push(name.to_owned());
                }
            }
            names.sort();
            Ok(names)
        })
    }
    fn get<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            match fs::read(self.resolve(directory, Some(name))?) {
                Ok(bytes) => Ok(Some(bytes)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(convert(e)),
            }
        })
    }
    fn put<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let path = self.resolve(directory, Some(name))?;
            let temporary = stage(&path, &bytes)?;
            if let Err(e) = fs::rename(&temporary, &path) {
                let _ = fs::remove_file(&temporary);
                return Err(convert(e));
            }
            sync_directory(&path);
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
            let path = self.resolve(directory, Some(name))?;
            if path.exists() {
                return Ok(Created::Exists);
            }
            let temporary = stage(&path, &bytes)?;
            let outcome = match fs::hard_link(&temporary, &path) {
                Ok(()) => Ok(Created::Created),
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Created::Exists),
                // No hard links here (FAT, some SMB mounts): the check above is the best this
                // file system offers, and the rename at least keeps the object whole.
                Err(_) if !path.exists() => fs::rename(&temporary, &path)
                    .map(|()| Created::Created)
                    .map_err(convert),
                Err(_) => Ok(Created::Exists),
            };
            let _ = fs::remove_file(&temporary);
            if outcome.is_ok() {
                sync_directory(&path);
            }
            outcome
        })
    }
    fn delete<'a>(&'a self, directory: &'a [String], name: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            match fs::remove_file(self.resolve(directory, Some(name))?) {
                Ok(()) => {
                    sync_directory(&self.resolve(directory, Some(name))?);
                    Ok(true)
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(convert(e)),
            }
        })
    }
}
