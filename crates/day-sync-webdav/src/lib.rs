// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A `WebDAV` folder as a [`day_sync::Store`].
//!
//! Collections are the engine's directories, resources its objects. Create-only PUTs use
//! `If-None-Match: *`. Named files are verified under a temporary name and published with
//! a no-overwrite MOVE. A complete `PROPFIND` at depth one is the listing, and a Multi-Status
//! that omits the collection itself or reports any member with a status other than 200 is
//! refused as incomplete rather than read as a shorter listing. Day's native HTTP stack
//! carries the requests by default. The `reqwest` feature enables that provider as an option.
#![cfg(not(target_arch = "wasm32"))]

pub use day_sync;
pub use day_webdav as webdav;

use day_sync::store::FileCreation;
use day_sync::{Created, Store, StoreError, StoreErrorKind, StoreFuture};
use day_webdav::{Client, Condition, Depth, Error as DavError, ResourcePath};
use sha2::{Digest, Sha256};

/// A [`Client`] bound to one account and base collection, serving as a store.
#[derive(Clone)]
pub struct WebDavStore {
    client: Client,
    private_directory: Option<(String, String)>,
}
impl WebDavStore {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            private_directory: None,
        }
    }
    /// Route sync records into one private collection, leaving the application's public
    /// file collection directly underneath the endpoint. Choose the layout at enrollment.
    /// # Errors
    /// Returns an error if the collection names are invalid or equal.
    pub fn with_private_directory(
        mut self,
        private: &str,
        public: &str,
    ) -> Result<Self, StoreError> {
        ResourcePath::from_segments([private, public], true).map_err(convert)?;
        if private.eq_ignore_ascii_case(public) {
            return Err(StoreError::new(StoreErrorKind::Invalid));
        }
        self.private_directory = Some((private.into(), public.into()));
        Ok(self)
    }
    fn directory<'a>(&self, directory: &'a [String]) -> std::borrow::Cow<'a, [String]> {
        if let Some((private, public)) = &self.private_directory
            && directory.first() != Some(public)
        {
            return std::borrow::Cow::Owned(
                std::iter::once(private.clone())
                    .chain(directory.iter().cloned())
                    .collect(),
            );
        }
        std::borrow::Cow::Borrowed(directory)
    }
    /// Authenticate and exercise the conditional writes required for synchronization.
    /// # Errors
    /// Reports authentication, permissions and protocol failures as structured store errors.
    pub async fn check_writable_collection(&self) -> Result<(), StoreError> {
        self.client
            .check_writable_collection()
            .await
            .map_err(convert)
    }
    /// The client, for requests outside the sync protocol such as a connectivity probe.
    pub fn client(&self) -> &Client {
        &self.client
    }
    pub fn into_client(self) -> Client {
        self.client
    }
}

fn path(directory: &[String], name: Option<&str>) -> Result<ResourcePath, StoreError> {
    ResourcePath::from_segments(
        directory.iter().map(String::as_str).chain(name),
        name.is_none(),
    )
    .map_err(convert)
}

/// The engine's classification of a DAV failure. The DAV error stays attached as the source.
fn convert(error: DavError) -> StoreError {
    use StoreErrorKind as K;
    let (kind, retry_after) = match &error {
        DavError::Status {
            status,
            retry_after,
            ..
        } => (
            match status {
                401 => K::Unauthorized,
                403 => K::Forbidden,
                404 => K::NotFound,
                507 => K::InsufficientStorage,
                423 | 429 | 500..=599 => K::Unavailable,
                _ => K::Protocol,
            },
            *retry_after,
        ),
        DavError::Transport {
            source: day_webdav::http::HttpError::BodyTooLarge { .. },
            ..
        }
        | DavError::Limit => (K::Limit, None),
        DavError::Transport { .. } | DavError::Deferred(_) => (K::Unavailable, None),
        DavError::LocalIo(e) => (
            match e.kind() {
                std::io::ErrorKind::StorageFull | std::io::ErrorKind::QuotaExceeded => {
                    K::InsufficientStorage
                }
                _ => K::Other,
            },
            None,
        ),
        DavError::InvalidEndpoint
        | DavError::InvalidPath
        | DavError::UnsafeHref
        | DavError::InvalidEntityTag => (K::Invalid, None),
        DavError::Xml | DavError::Redirect { .. } | DavError::Partial(_) | DavError::Protocol => {
            (K::Protocol, None)
        }
    };
    StoreError::with_source(kind, error).retry_after(retry_after)
}

/// Compare the temporary remote resource to the immutable source before exposing its name.
struct CompareSource {
    source: std::io::BufReader<std::fs::File>,
    mismatch: bool,
    hash: Sha256,
    bytes: u64,
}
impl std::io::Write for CompareSource {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        use std::io::BufRead;
        let expected = self.source.fill_buf()?;
        let n = expected.len().min(bytes.len());
        if !bytes.is_empty() && (n == 0 || expected[..n] != bytes[..n]) {
            self.mismatch = true;
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        self.source.consume(n);
        self.hash.update(&bytes[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Store for WebDavStore {
    fn endpoint(&self) -> String {
        self.client.endpoint().as_str().to_owned()
    }
    fn ensure_directory<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            for n in 1..=directory.len() {
                let collection = path(&directory[..n], None)?;
                match self.client.mkcol(&collection).await {
                    Ok(_) => {}
                    // Already there, or a server that answers an existing collection this
                    // way; a depth-zero PROPFIND settles which.
                    Err(DavError::Status {
                        status: 403 | 405 | 409 | 423,
                        ..
                    }) => {
                        let listing = self
                            .client
                            .propfind(&collection, Depth::Zero)
                            .await
                            .map_err(convert)?;
                        if listing.resources.len() != 1 || !listing.resources[0].is_collection() {
                            return Err(StoreError::new(StoreErrorKind::IncompleteListing));
                        }
                    }
                    Err(e) => return Err(convert(e)),
                }
            }
            Ok(())
        })
    }
    fn list<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            let collection = path(directory, None)?;
            let listing = self
                .client
                .propfind(&collection, Depth::One)
                .await
                .map_err(convert)?;
            let root = self.client.endpoint().url(&collection);
            if !listing
                .resources
                .iter()
                .any(|r| r.href == root && r.is_collection())
                || listing
                    .resources
                    .iter()
                    .any(|r| r.status.is_some_and(|s| s != 200))
            {
                return Err(StoreError::new(StoreErrorKind::IncompleteListing));
            }
            listing
                .resources
                .iter()
                .filter(|r| r.href != root)
                .map(|r| {
                    let child = r
                        .href
                        .strip_prefix(&root)
                        .ok_or_else(|| StoreError::new(StoreErrorKind::Protocol))?;
                    percent_encoding::percent_decode_str(child)
                        .decode_utf8()
                        .map(std::borrow::Cow::into_owned)
                        .map_err(|_| StoreError::new(StoreErrorKind::Protocol))
                })
                .collect()
        })
    }
    fn get<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            match self.client.get(&path(directory, Some(name))?, None).await {
                Ok(response) => Ok(Some(response.body)),
                Err(DavError::Status { status: 404, .. }) => Ok(None),
                Err(e) => Err(convert(e)),
            }
        })
    }
    fn read_object<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        validator: Option<&'a str>,
    ) -> StoreFuture<'a, day_sync::store::ObjectRead> {
        use day_sync::store::ObjectRead;
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            let path = path(directory, Some(name))?;
            let tag = validator
                .and_then(|v| webdav::EntityTag::parse(v).ok())
                .filter(|v| !v.is_weak());
            let result = self.client.get(&path, tag.as_ref()).await;
            let response = match result {
                Err(DavError::Status { status: 404, .. }) => return Ok(ObjectRead::Absent),
                other => other.map_err(convert)?,
            };
            // If-None-Match permits a weak comparison. Only a matching strong tag proves
            // that the byte representation whose digest we checked is still current.
            let response = if response.status == 304 {
                if tag.is_some()
                    && response.header("ETag") == tag.as_ref().map(webdav::EntityTag::as_str)
                {
                    return Ok(ObjectRead::Unchanged);
                }
                self.client.get(&path, None).await.map_err(convert)?
            } else {
                response
            };
            let validator = response
                .header("ETag")
                .and_then(|s| webdav::EntityTag::parse(s).ok())
                .filter(|t| !t.is_weak())
                .map(|t| t.as_str().to_owned());
            Ok(ObjectRead::Bytes(response.body, validator))
        })
    }
    fn put<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            self.client
                .put(
                    &path(directory, Some(name))?,
                    bytes,
                    Condition::Unconditional,
                )
                .await
                .map(|_| ())
                .map_err(convert)
        })
    }
    fn create<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        bytes: Vec<u8>,
    ) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            match self
                .client
                .put(&path(directory, Some(name))?, bytes, Condition::CreateOnly)
                .await
            {
                Ok(_) => Ok(Created::Created),
                Err(DavError::Status { status: 412, .. }) => Ok(Created::Exists),
                Err(e) => Err(convert(e)),
            }
        })
    }
    fn create_file<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        file: &'a std::path::Path,
        limit: u64,
    ) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            self.create_file_with_progress(directory, name, file, limit, &mut |_| {})
                .await
                .map(Into::into)
        })
    }
    fn create_file_with_progress<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        file: &'a std::path::Path,
        limit: u64,
        progress: &'a mut dyn FnMut(u64),
    ) -> StoreFuture<'a, FileCreation> {
        Box::pin(async move {
            use std::io::BufRead;
            let destination = path(&self.directory(directory), Some(name))?;
            let source = std::fs::File::open(file).map_err(StoreError::local_io)?;
            if source.metadata().map_err(StoreError::local_io)?.len() > limit {
                return Err(StoreError::new(StoreErrorKind::Limit));
            }
            let staging = ["day-transfers-v1".into()];
            self.ensure_directory(&staging).await?;
            let temporary = path(
                &self.directory(&staging),
                Some(&uuid::Uuid::new_v4().to_string()),
            )?;
            let put = self
                .client
                .create_file_with_progress(&temporary, file, limit, progress)
                .await;
            if matches!(put, Err(DavError::Status { status: 412, .. })) {
                // Even a UUID collision cannot give us ownership of an existing staging file.
                return Err(StoreError::new(StoreErrorKind::Protocol));
            }
            let publication = async {
                let mut check = CompareSource {
                    source: std::io::BufReader::new(source),
                    mismatch: false,
                    hash: Sha256::new(),
                    bytes: 0,
                };
                if let Err(read_error) = self.client.read_file(&temporary, &mut check, limit).await
                {
                    put.map_err(convert)?;
                    if check.mismatch {
                        return Err(StoreError::new(StoreErrorKind::Protocol));
                    }
                    return Err(convert(read_error));
                }
                if !check
                    .source
                    .fill_buf()
                    .map_err(StoreError::local_io)?
                    .is_empty()
                {
                    put.map_err(convert)?;
                    return Err(StoreError::new(StoreErrorKind::Protocol));
                }
                match self
                    .client
                    .move_resource(&temporary, &destination, false)
                    .await
                {
                    Ok(_) => Ok(FileCreation::Verified {
                        sha256: check.hash.finalize().into(),
                        bytes: check.bytes,
                    }),
                    Err(DavError::Status { status: 412, .. }) => Ok(FileCreation::Exists),
                    Err(e) => Err(convert(e)),
                }
            }
            .await;
            if !matches!(publication, Ok(FileCreation::Verified { .. })) {
                let _ = self
                    .client
                    .delete(&temporary, Condition::Unconditional)
                    .await;
            }
            publication
        })
    }
    fn read_file<'a>(
        &'a self,
        directory: &'a [String],
        name: &'a str,
        sink: &'a mut dyn std::io::Write,
        limit: u64,
    ) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            match self
                .client
                .read_file(&path(directory, Some(name))?, sink, limit)
                .await
            {
                Ok(()) => Ok(true),
                Err(DavError::Status { status: 404, .. }) => Ok(false),
                Err(e) => Err(convert(e)),
            }
        })
    }
    fn delete<'a>(&'a self, directory: &'a [String], name: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let mapped = self.directory(directory);
            let directory = mapped.as_ref();
            match self
                .client
                .delete(&path(directory, Some(name))?, Condition::Unconditional)
                .await
            {
                Ok(_) => Ok(true),
                Err(DavError::Status { status: 404, .. }) => Ok(false),
                Err(e) => Err(convert(e)),
            }
        })
    }
}
