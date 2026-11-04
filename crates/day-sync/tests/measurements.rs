// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Reproducible synthetic measurements; not a production transport benchmark.
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::block_on;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};
#[derive(Default)]
struct Metrics {
    operations: BTreeMap<&'static str, usize>,
    read_bytes: usize,
    write_bytes: usize,
}
struct Measured {
    inner: MemoryStore,
    metrics: Mutex<Metrics>,
}
impl Measured {
    fn count(&self, op: &'static str) {
        *self
            .metrics
            .lock()
            .unwrap()
            .operations
            .entry(op)
            .or_default() += 1;
    }
    fn take(&self) -> Metrics {
        std::mem::take(&mut *self.metrics.lock().unwrap())
    }
}
impl Store for Measured {
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }
    fn ensure_directory<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, ()> {
        self.count("mkdir");
        self.inner.ensure_directory(d)
    }
    fn list<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        self.count("list");
        self.inner.list(d)
    }
    fn get<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.count("get");
            let value = self.inner.get(d, n).await?;
            self.metrics.lock().unwrap().read_bytes += value.as_ref().map_or(0, Vec::len);
            Ok(value)
        })
    }
    fn put<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, ()> {
        self.count("put");
        self.metrics.lock().unwrap().write_bytes += b.len();
        self.inner.put(d, n, b)
    }
    fn create<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, Created> {
        self.count("create");
        self.metrics.lock().unwrap().write_bytes += b.len();
        self.inner.create(d, n, b)
    }
    fn delete<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, bool> {
        self.count("delete");
        self.inner.delete(d, n)
    }
}
#[test]
#[ignore = "synthetic sizing and request measurement; run explicitly with --ignored --nocapture"]
fn bootstrap_and_cleanup_at_ten_and_fifty_thousand_objects() {
    block_on(async {
        for count in [10_000, 50_000] {
            let dir = tempfile::tempdir().unwrap();
            let identity = Arc::new(Identity::create_space().unwrap());
            let a = Replica::open(dir.path().join("a"), identity.clone())
                .await
                .unwrap();
            let store = Measured {
                inner: MemoryStore::new(),
                metrics: Mutex::default(),
            };
            let start = Instant::now();
            a.commit(
                (1..=count)
                    .map(|n| Change {
                        id: uuid::Uuid::from_u128(n).to_string(),
                        value: if n % 10 == 0 {
                            None
                        } else {
                            Some(BookmarkValue {
                                url: format!("synthetic://measurement/{n}"),
                                title: format!("Synthetic object {n}"),
                            })
                        },
                    })
                    .collect(),
            )
            .await
            .unwrap();
            let commit_ms = start.elapsed().as_millis();
            let records = a.local_objects().await.unwrap().len();
            let start = Instant::now();
            a.sync(&store).await.unwrap();
            let publish_ms = start.elapsed().as_millis();
            let publication = store.take();
            let objects = store
                .inner
                .objects(&[format!("day-sync-v5-{}", identity.space.id)]);
            let manifest: serde_json::Value = serde_json::from_slice(
                objects
                    .iter()
                    .find(|(n, _)| n.starts_with("snap-") && !n.contains(".part-"))
                    .unwrap()
                    .1,
            )
            .unwrap();
            let b = Replica::open(
                dir.path().join("b"),
                Arc::new(Identity::join(identity.space.clone()).unwrap()),
            )
            .await
            .unwrap();
            let start = Instant::now();
            b.sync(&store).await.unwrap();
            let bootstrap_ms = start.elapsed().as_millis();
            let bootstrap = store.take();
            assert_eq!(b.snapshot().await.unwrap().candidates.len(), count as usize);
            let start = Instant::now();
            b.sync(&store).await.unwrap();
            let idle_ms = start.elapsed().as_millis();
            let idle = store.take();
            a.close().await.unwrap();
            b.close().await.unwrap();
            println!(
                "objects={count} tombstones={} records={records} state_bytes={} remote_names={} commit_ms={commit_ms} publish_ms={publish_ms} bootstrap_ms={bootstrap_ms} idle_ms={idle_ms} db_bytes={} publication_ops={:?} publication_read={} publication_write={} bootstrap_ops={:?} bootstrap_read={} idle_ops={:?} idle_read={}",
                count / 10,
                manifest["bytes"],
                objects.len(),
                std::fs::metadata(dir.path().join("a")).unwrap().len(),
                publication.operations,
                publication.read_bytes,
                publication.write_bytes,
                bootstrap.operations,
                bootstrap.read_bytes,
                idle.operations,
                idle.read_bytes
            );
        }
    });
}
