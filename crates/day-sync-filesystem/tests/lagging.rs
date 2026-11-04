// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
//! A synthetic folder carrier exposes a delayed name view and resurrects deleted records.
use day_sync::*;
use day_sync_filesystem::FilesystemStore;
use futures_lite::future::block_on;
use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
struct Lagging {
    inner: FilesystemStore,
    hidden: Mutex<BTreeSet<String>>,
    delay: AtomicBool,
    resurrect: AtomicBool,
}
impl Store for Lagging {
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }
    fn ensure_directory<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, ()> {
        self.inner.ensure_directory(d)
    }
    fn list<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        Box::pin(async move {
            let mut v = self.inner.list(d).await?;
            v.retain(|n| !self.hidden.lock().unwrap().contains(n));
            Ok(v)
        })
    }
    fn get<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, Option<Vec<u8>>> {
        self.inner.get(d, n)
    }
    fn put<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, ()> {
        self.inner.put(d, n, b)
    }
    fn create<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            let r = self.inner.create(d, n, b).await?;
            if self.delay.load(Ordering::SeqCst) && n.starts_with("snap-") {
                self.hidden.lock().unwrap().insert(n.into());
            }
            Ok(r)
        })
    }
    fn delete<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            let bytes = self.inner.get(d, n).await?;
            let r = self.inner.delete(d, n).await?;
            if self.resurrect.load(Ordering::SeqCst)
                && n.starts_with("tx-")
                && let Some(bytes) = bytes
            {
                self.inner.put(d, n, bytes).await?;
            }
            Ok(r)
        })
    }
}
#[test]
fn deleted_inputs_visible_before_snapshot_and_resurrection_are_eventually_safe() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Arc::new(Identity::create_space().unwrap());
        let a = Replica::open(d.path().join("a"), identity.clone())
            .await
            .unwrap();
        let store = Lagging {
            inner: FilesystemStore::open(d.path().join("share")).unwrap(),
            hidden: Mutex::default(),
            delay: AtomicBool::new(false),
            resurrect: AtomicBool::new(false),
        };
        let x = uuid::Uuid::new_v4().to_string();
        a.commit(vec![Change {
            id: x.clone(),
            value: Some(BookmarkValue {
                url: "synthetic://lag".into(),
                title: "old".into(),
            }),
        }])
        .await
        .unwrap();
        a.sync(&store).await.unwrap();
        a.commit(vec![Change { id: x, value: None }]).await.unwrap();
        a.sync(&store).await.unwrap();
        store.delay.store(true, Ordering::SeqCst);
        a.compact(&store).await.unwrap();
        let b = Replica::open(
            d.path().join("b"),
            Arc::new(Identity::join(identity.space.clone()).unwrap()),
        )
        .await
        .unwrap();
        b.sync(&store).await.unwrap();
        assert!(
            b.snapshot().await.unwrap().frontier.is_empty(),
            "unknown absent snapshot cannot be inferred"
        );
        store.delay.store(false, Ordering::SeqCst);
        store.hidden.lock().unwrap().clear();
        b.sync(&store).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        store.resurrect.store(true, Ordering::SeqCst);
        for i in 0..32 {
            a.commit(vec![Change {
                id: uuid::Uuid::new_v4().to_string(),
                value: Some(BookmarkValue {
                    url: "synthetic://lag".into(),
                    title: i.to_string(),
                }),
            }])
            .await
            .unwrap();
            a.sync(&store).await.unwrap();
            if i % 4 == 0 {
                a.compact(&store).await.unwrap();
            }
            b.sync(&store).await.unwrap();
        }
        store.resurrect.store(false, Ordering::SeqCst);
        a.compact(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
