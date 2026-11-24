// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::block_on;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ReplaceListedHistory {
    inner: MemoryStore,
    peer: Replica,
    armed: AtomicBool,
    compact: bool,
    ghost: bool,
    lists: AtomicUsize,
    reads: AtomicUsize,
    trigger_after: usize,
}
impl Store for ReplaceListedHistory {
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }
    fn ensure_directory<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, ()> {
        self.inner.ensure_directory(d)
    }
    fn list<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        self.inner.list(d)
    }
    fn get<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            if n.starts_with("tx-") {
                if self.ghost {
                    return Ok(None);
                }
                if self.reads.fetch_add(1, Ordering::SeqCst) == self.trigger_after
                    && self.armed.swap(false, Ordering::SeqCst)
                {
                    if self.compact {
                        self.peer.compact(&self.inner).await.unwrap();
                    } else {
                        self.inner.lose(d, n);
                    }
                }
            }
            self.inner.get(d, n).await
        })
    }
    fn put<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, ()> {
        self.inner.put(d, n, b)
    }
    fn create<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, Created> {
        self.inner.create(d, n, b)
    }
    fn delete<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, bool> {
        self.inner.delete(d, n)
    }
}

#[test]
fn peer_compaction_between_listing_and_get_is_transparent_to_sync_and_compact() {
    block_on(async {
        for (force, trigger_after) in [(false, 0), (false, 2), (true, 0), (true, 2)] {
            let d = tempfile::tempdir().unwrap();
            let identity = Identity::create_space().unwrap();
            let peer = Replica::open(d.path().join("peer"), Arc::new(identity.clone()))
                .await
                .unwrap();
            let reader = Replica::open(
                d.path().join("reader"),
                Arc::new(Identity::join(identity.space).unwrap()),
            )
            .await
            .unwrap();
            let inner = MemoryStore::new();
            for title in ["synthetic first", "synthetic second", "synthetic last"] {
                peer.commit(vec![Change {
                    id: "00000000-0000-4000-8000-000000000001".into(),
                    value: Some(BookmarkValue {
                        url: "synthetic://listing-race".into(),
                        title: title.into(),
                    }),
                }])
                .await
                .unwrap();
            }
            peer.sync(&inner).await.unwrap();
            let expected = peer.snapshot().await.unwrap();
            let store = ReplaceListedHistory {
                inner,
                peer,
                armed: AtomicBool::new(true),
                compact: true,
                ghost: false,
                lists: AtomicUsize::new(0),
                reads: AtomicUsize::new(0),
                trigger_after,
            };
            let options = SyncOptions {
                max_deletions: 1,
                ..SyncOptions::default()
            };
            let report = if force {
                reader.compact_with_options(&store, options).await
            } else {
                reader.sync_with_options(&store, options).await
            }
            .unwrap();
            let actual = reader.snapshot().await.unwrap();
            assert_eq!(actual.candidates, expected.candidates);
            assert_eq!(actual.bookmarks, expected.bookmarks);
            assert_eq!(actual.waiting, 0);
            assert_eq!(actual.pending_uploads, 0);
            assert!(!actual.poisoned);
            assert!(report.deletion_requests <= 1);
            assert!(!store.armed.load(Ordering::SeqCst));
            assert!(store.lists.load(Ordering::SeqCst) >= 2);
            reader.close().await.unwrap();
            store.peer.close().await.unwrap();
        }
    });
}

#[test]
fn disappearing_history_without_a_covering_snapshot_is_not_success() {
    block_on(async {
        for ghost in [false, true] {
            let d = tempfile::tempdir().unwrap();
            let identity = Identity::create_space().unwrap();
            let peer = Replica::open(d.path().join("peer"), Arc::new(identity.clone()))
                .await
                .unwrap();
            let reader = Replica::open(
                d.path().join("reader"),
                Arc::new(Identity::join(identity.space).unwrap()),
            )
            .await
            .unwrap();
            let inner = MemoryStore::new();
            peer.commit(vec![Change {
                id: "00000000-0000-4000-8000-000000000001".into(),
                value: Some(BookmarkValue {
                    url: "synthetic://listing-race".into(),
                    title: "must not vanish".into(),
                }),
            }])
            .await
            .unwrap();
            peer.sync(&inner).await.unwrap();
            let store = ReplaceListedHistory {
                inner,
                peer,
                armed: AtomicBool::new(true),
                compact: false,
                ghost,
                lists: AtomicUsize::new(0),
                reads: AtomicUsize::new(0),
                trigger_after: 0,
            };
            assert!(
                matches!(reader.sync(&store).await, Err(Error::Store(e)) if e.kind == StoreErrorKind::NotFound)
            );
            assert!(
                store.lists.load(Ordering::SeqCst) <= 3,
                "refresh must be bounded"
            );
            assert!(!reader.snapshot().await.unwrap().poisoned);
            reader.close().await.unwrap();
            store.peer.close().await.unwrap();
        }
    });
}
