// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A WebDAV folder as a [`day_sync::Store`].
//!
//! Collections are the engine's directories, resources its objects. Create-only writes use
//! `If-None-Match: *`; a complete `PROPFIND` at depth one is the listing, and a Multi-Status
//! that omits the collection itself or reports any member with a status other than 200 is
//! refused as incomplete rather than read as a shorter listing. Day's native HTTP stack
//! carries the requests; the `reqwest` feature selects that provider instead.
#![cfg(not(target_arch = "wasm32"))]

pub use day_sync;
pub use day_webdav as webdav;

use day_sync::{Created, Store, StoreError, StoreErrorKind, StoreFuture};
use day_webdav::{Client, Condition, Depth, Error as DavError, ResourcePath};

/// A [`Client`] bound to one account and base collection, serving as a store.
#[derive(Clone)]
pub struct WebDavStore {
    client: Client,
}
impl WebDavStore {
    pub fn new(client: Client) -> Self {
        Self { client }
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
        DavError::Transport { .. } | DavError::Deferred(_) => (K::Unavailable, None),
        DavError::Limit => (K::Limit, None),
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

impl Store for WebDavStore {
    fn endpoint(&self) -> String {
        self.client.endpoint().as_str().to_owned()
    }
    fn ensure_directory<'a>(&'a self, directory: &'a [String]) -> StoreFuture<'a, ()> {
        Box::pin(async move {
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
                    r.href
                        .strip_prefix(&root)
                        .map(str::to_owned)
                        .ok_or_else(|| StoreError::new(StoreErrorKind::Protocol))
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
            match self.client.get(&path(directory, Some(name))?, None).await {
                Ok(response) => Ok(Some(response.body)),
                Err(DavError::Status { status: 404, .. }) => Ok(None),
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
    fn delete<'a>(&'a self, directory: &'a [String], name: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
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
