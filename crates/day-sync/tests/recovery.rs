// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Synthetic recovery scenarios from the independent implementation comparison.
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::{block_on, poll_once, yield_now};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};
#[derive(Default, Debug)]
struct Stats {
    ops: BTreeMap<&'static str, usize>,
    read: usize,
    written: usize,
}
struct Meter {
    inner: MemoryStore,
    stats: Mutex<Stats>,
    pause: AtomicU8,
    limit: AtomicUsize,
    oversized_manifest: AtomicBool,
}
impl Meter {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            stats: Mutex::default(),
            pause: AtomicU8::new(0),
            limit: AtomicUsize::new(usize::MAX),
            oversized_manifest: AtomicBool::new(false),
        }
    }
    fn count(&self, s: &'static str) {
        *self.stats.lock().unwrap().ops.entry(s).or_default() += 1;
    }
    fn take(&self) -> Stats {
        std::mem::take(&mut *self.stats.lock().unwrap())
    }
}
impl Store for Meter {
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
            let b = if self.oversized_manifest.load(Ordering::SeqCst)
                && n.starts_with("snap-")
                && !n.contains(".part-")
            {
                Some(vec![b'x'; 65_537])
            } else {
                self.inner.get(d, n).await?
            };
            self.stats.lock().unwrap().read += b.as_ref().map_or(0, Vec::len);
            Ok(b)
        })
    }
    fn put<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, ()> {
        self.count("put");
        self.stats.lock().unwrap().written += b.len();
        Box::pin(async move {
            let limit = self.limit.load(Ordering::SeqCst);
            if limit != usize::MAX {
                let files = self.inner.objects(d);
                if !files.contains_key(n) && files.len() >= limit {
                    return Err(StoreError::new(StoreErrorKind::InsufficientStorage));
                }
            }
            self.inner.put(d, n, b).await
        })
    }
    fn create<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, Created> {
        self.count("create");
        self.stats.lock().unwrap().written += b.len();
        Box::pin(async move {
            let limit = self.limit.load(Ordering::SeqCst);
            if limit != usize::MAX {
                let files = self.inner.objects(d);
                if !files.contains_key(n) && files.len() >= limit {
                    return Err(StoreError::new(StoreErrorKind::InsufficientStorage));
                }
            }
            self.inner.create(d, n, b).await
        })
    }
    fn delete<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            std::future::poll_fn(|_| {
                if self.pause.load(Ordering::SeqCst) != 0 {
                    self.pause.store(2, Ordering::SeqCst);
                    std::task::Poll::Pending
                } else {
                    std::task::Poll::Ready(())
                }
            })
            .await;
            self.count("delete");
            self.inner.delete(d, n).await
        })
    }
}
fn edit(n: u128, v: &str) -> Change {
    Change {
        id: uuid::Uuid::from_u128(n).to_string(),
        value: Some(BookmarkValue {
            url: "synthetic://review".into(),
            title: v.into(),
        }),
    }
}
fn directory(r: &Replica) -> Vec<String> {
    vec![format!("day-sync-v5-{}", r.space())]
}
async fn open(p: &std::path::Path, name: &str, s: &Space) -> Replica {
    Replica::open(p.join(name), Arc::new(Identity::join(s.clone()).unwrap()))
        .await
        .unwrap()
}
fn result<T>(r: Result<T, Error>) -> String {
    match r {
        Ok(_) => "ok".into(),
        Err(e) => format!("{e:?}"),
    }
}
fn count_records(s: &Meter, d: &[String]) -> usize {
    s.inner
        .objects(d)
        .keys()
        .filter(|n| n.starts_with("tx-"))
        .count()
}
fn part(s: &Meter, d: &[String]) -> String {
    s.inner
        .objects(d)
        .keys()
        .find(|n| n.contains(".part-"))
        .unwrap()
        .clone()
}

#[test]
fn comparison_regressions() {
    block_on(async {
        {
            // Loss of a previously verified part, without any fresh edit.
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            let a = open(d.path(), "a", &id.space).await;
            a.commit(vec![edit(1, "retained")]).await.unwrap();
            a.sync(&s).await.unwrap();
            a.compact(&s).await.unwrap();
            let dir = directory(&a);
            s.inner.lose(&dir, &part(&s, &dir));
            let first = result(a.sync(&s).await);
            let b = open(d.path(), "b", &id.space).await;
            let fresh = result(b.sync(&s).await);
            assert_eq!(first, "ok");
            assert_eq!(fresh, "ok");
            assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 1);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // A process stops after verification, then the replacement loses a part before retry.
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            let armed = Arc::new(AtomicBool::new(false));
            let flag = armed.clone();
            let a = Replica::open_with_probe(
                d.path().join("a"),
                id.clone(),
                Arc::new(move |p| {
                    if p == Checkpoint::SnapshotVerified && flag.swap(false, Ordering::SeqCst) {
                        Err(day_persistence::DbError::driver("synthetic interruption"))
                    } else {
                        Ok(())
                    }
                }),
            )
            .await
            .unwrap();
            a.commit(vec![edit(1, "survive retry")]).await.unwrap();
            a.sync(&s).await.unwrap();
            armed.store(true, Ordering::SeqCst);
            assert!(a.compact(&s).await.is_err());
            let dir = directory(&a);
            let before = count_records(&s, &dir);
            s.inner.lose(&dir, &part(&s, &dir));
            a.close().await.unwrap();
            drop(a);
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            let resumed = result(a.sync(&s).await);
            let b = open(d.path(), "b", &id.space).await;
            let fresh = result(b.sync(&s).await);
            assert_eq!(before, 1);
            assert_eq!(resumed, "ok");
            assert_eq!(fresh, "ok");
            assert_eq!(count_records(&s, &dir), 0);
            assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 1);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // Freeze while compaction is awaiting admission of its first delete.
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            let a = open(d.path(), "a", &id.space).await;
            a.commit(vec![edit(1, "freeze")]).await.unwrap();
            a.sync(&s).await.unwrap();
            s.pause.store(1, Ordering::SeqCst);
            let mut f = Box::pin(a.compact(&s));
            while s.pause.load(Ordering::SeqCst) != 2 {
                assert!(poll_once(&mut f).await.is_none());
                yield_now().await;
            }
            let freeze = result(a.freeze(&uuid::Uuid::new_v4().to_string()).await);
            let before = count_records(&s, &directory(&a));
            s.pause.store(0, Ordering::SeqCst);
            let resumed = result(f.await);
            assert_eq!(freeze, "Busy");
            assert_eq!(resumed, "ok");
            assert_eq!(before, 1);
            assert_eq!(count_records(&s, &directory(&a)), 0);
            a.close().await.unwrap();
        }
        {
            // Old manifest reappears without its parts, while its complete successor survives.
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            let a = open(d.path(), "a", &id.space).await;
            a.commit(vec![edit(1, "old")]).await.unwrap();
            a.compact(&s).await.unwrap();
            let dir = directory(&a);
            let old = s
                .inner
                .objects(&dir)
                .into_iter()
                .find(|(n, _)| n.starts_with("snap-") && !n.contains(".part-"))
                .unwrap();
            a.commit(vec![edit(1, "current")]).await.unwrap();
            a.compact(&s).await.unwrap();
            s.inner.put(&dir, &old.0, old.1).await.unwrap();
            let b = open(d.path(), "b", &id.space).await;
            let first = result(b.sync(&s).await);
            b.commit(vec![edit(2, "new local work")]).await.unwrap();
            let second = result(b.sync(&s).await);
            let pending = b.snapshot().await.unwrap().pending_uploads;
            assert_eq!(first, "ok");
            assert_eq!(second, "ok");
            assert_eq!(pending, 0);
            assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 2);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // A covered raw record contradicts the retained head.
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            let b = Replica::open(d.path().join("b"), id).await.unwrap();
            a.commit(vec![edit(1, "a")]).await.unwrap();
            b.commit(vec![edit(1, "b")]).await.unwrap();
            a.compact(&s).await.unwrap();
            let o = b.local_objects().await.unwrap().remove(0);
            s.inner
                .put(&directory(&a), &o.filename, o.bytes)
                .await
                .unwrap();
            assert!(matches!(a.sync(&s).await, Err(Error::Fork)));
            assert!(a.snapshot().await.unwrap().poisoned);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // A snapshot at the same frontier contradicts this writer's head.
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let sa = Meter::new();
            let sb = Meter::new();
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            let b = Replica::open(d.path().join("b"), id).await.unwrap();
            a.commit(vec![edit(1, "a")]).await.unwrap();
            b.commit(vec![edit(1, "b")]).await.unwrap();
            a.compact(&sa).await.unwrap();
            b.compact(&sb).await.unwrap();
            for (n, bytes) in sb.inner.objects(&directory(&b)) {
                if n.starts_with("snap-") {
                    sa.inner.put(&directory(&a), &n, bytes).await.unwrap();
                }
            }
            assert!(matches!(a.sync(&sa).await, Err(Error::Fork)));
            assert!(a.snapshot().await.unwrap().poisoned);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // An invalid object merely has a syntactically valid future own-actor name.
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            let a = open(d.path(), "a", &id.space).await;
            a.sync(&s).await.unwrap();
            let name = format!("tx-{}.{:020}.{}.ds5", a.actor(), 2, "a".repeat(64));
            s.inner
                .put(&directory(&a), &name, b"synthetic invalid bytes".to_vec())
                .await
                .unwrap();
            assert!(matches!(
                a.sync(&s).await,
                Err(Error::RemoteObjectsRejected { count: 1 })
            ));
            assert!(!a.snapshot().await.unwrap().poisoned);
            a.close().await.unwrap();
        }
        {
            // Lost individual published record, no snapshot yet.
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            let a = open(d.path(), "a", &id.space).await;
            a.commit(vec![edit(1, "repair")]).await.unwrap();
            a.sync(&s).await.unwrap();
            let dir = directory(&a);
            let n = s
                .inner
                .objects(&dir)
                .keys()
                .find(|n| n.starts_with("tx-"))
                .unwrap()
                .clone();
            s.inner.lose(&dir, &n);
            let one = result(a.sync(&s).await);
            let two = result(a.sync(&s).await);
            let b = open(d.path(), "b", &id.space).await;
            let fresh = result(b.sync(&s).await);
            assert_eq!(one, "RemoteHistoryMissing");
            assert_eq!(two, "ok");
            assert_eq!(fresh, "ok");
            assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 1);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
        {
            // Same values/dots, different historical heads: receipt comparison.
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            let b = Replica::open(d.path().join("b"), id).await.unwrap();
            let token = uuid::Uuid::new_v4().to_string();
            for (r, v) in [(&a, "different a"), (&b, "different b")] {
                let s = Meter::new();
                r.commit(vec![edit(1, v)]).await.unwrap();
                r.commit(vec![edit(1, "same final value")]).await.unwrap();
                r.compact(&s).await.unwrap();
                r.freeze(&token).await.unwrap();
            }
            let ca = a.attest().await.unwrap().cut;
            let cb = b.attest().await.unwrap().cut;
            assert_ne!(ca, cb);
            assert_eq!(ca.state_hash, cb.state_hash);
            assert_ne!(ca.candidates_hash, cb.candidates_hash);
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn bounded_cleanup_reopens_repairs_and_preserves_new_edits() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Arc::new(Identity::create_space().unwrap());
        let s = Meter::new();
        let a = Replica::open(d.path().join("a"), identity.clone())
            .await
            .unwrap();
        // Publish without triggering automatic retention compaction, then give the receiver
        // provenance for every individual input. Synthetic external writer = fixture only.
        for i in 1..=300 {
            a.commit(vec![edit(i, "captured")]).await.unwrap();
        }
        let dir = directory(&a);
        s.ensure_directory(&dir).await.unwrap();
        for o in a.local_objects().await.unwrap() {
            s.put(&dir, &o.filename, o.bytes).await.unwrap();
        }
        // A separate receiver actually reads/applies the inputs; the writer's unacknowledged
        // outbox intentionally does not yet have deletion provenance.
        let identity_b = Arc::new(Identity::join(identity.space.clone()).unwrap());
        let b = Replica::open(d.path().join("b"), identity_b.clone())
            .await
            .unwrap();
        let options = SyncOptions {
            max_deletions: 33,
            ..SyncOptions::default()
        };
        let r = b.sync_with_options(&s, options).await.unwrap();
        assert_eq!(r.deletion_requests, 33);
        assert_eq!(r.pending_deletions, 267);
        assert_eq!(b.snapshot().await.unwrap().transactions, 0);
        b.close().await.unwrap();
        drop(b);
        // Delete a replacement part after verified progress is durable. Each resume must
        // repair it before issuing its next delete; a fresh client checks this each round.
        s.inner.lose(&dir, &part(&s, &dir));
        let b = Replica::open(d.path().join("b"), identity_b).await.unwrap();
        b.commit(vec![edit(301, "newer than capture")])
            .await
            .unwrap();
        for round in 0..10 {
            s.take();
            let r = b.sync_with_options(&s, options).await.unwrap();
            assert!(r.deletion_requests <= 33);
            assert!(s.take().ops.get("delete").copied().unwrap_or(0) <= 33);
            let fresh = open(d.path(), &format!("fresh{round}"), &identity.space).await;
            fresh.sync(&s).await.unwrap();
            assert_eq!(fresh.snapshot().await.unwrap().bookmarks.len(), 301);
            fresh.close().await.unwrap();
            if r.pending_deletions == 0 {
                break;
            }
            assert!(round < 9, "cleanup did not finish");
        }
        assert_eq!(count_records(&s, &dir), 1); // newer edit was never in the captured deletion set
        b.compact(&s).await.unwrap();
        assert_eq!(count_records(&s, &dir), 0);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn idle_cache_has_bounded_reads_and_periodic_or_forced_audits() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let id = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &id.space).await;
        a.commit((1..=1000).map(|n| edit(n, &"x".repeat(1000))).collect())
            .await
            .unwrap();
        a.compact(&s).await.unwrap();
        s.take();
        for _ in 0..15 {
            let r = a.sync(&s).await.unwrap();
            assert_eq!(r.snapshots_verified, 0);
            let st = s.take();
            assert_eq!(st.ops.get("list"), Some(&1));
            assert_eq!(st.ops.get("get"), Some(&2));
            assert!(st.read < 1024);
        }
        let r = a.sync(&s).await.unwrap();
        assert_eq!(r.snapshots_verified, 1);
        assert!(s.take().read > 1_000_000);
        let dir = directory(&a);
        s.inner
            .corrupt(&dir, &part(&s, &dir), b"synthetic corruption".to_vec());
        let r = a
            .sync_with_options(
                &s,
                SyncOptions {
                    verify_snapshots: true,
                    ..SyncOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(r.repaired_history);
        assert_eq!(r.snapshots_published, 1);
        s.inner.corrupt(
            &dir,
            &part(&s, &dir),
            b"synthetic corruption again".to_vec(),
        );
        for _ in 0..15 {
            assert!(!a.sync(&s).await.unwrap().repaired_history);
        }
        assert!(a.sync(&s).await.unwrap().repaired_history);
        s.inner.lose(&dir, &part(&s, &dir));
        assert!(a.sync(&s).await.unwrap().repaired_history);
        let b = open(d.path(), "b", &id.space).await;
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 1000);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn quota_with_snapshot_headroom_does_not_require_uploading_the_backlog() {
    block_on(async {
        for count in [0, 1, 63, 64, 65, 1000, 4095, 4096, 4200] {
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            s.limit.store(68, Ordering::SeqCst);
            let a = open(d.path(), "a", &id.space).await;
            for i in 0..count {
                a.commit(vec![edit(1, &i.to_string())]).await.unwrap();
            }
            let report = a.sync(&s).await.unwrap();
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
            assert_eq!(report.uploaded, if count < 64 { count } else { 0 });
            assert_eq!(
                count_records(&s, &directory(&a)),
                if count < 64 { count } else { 0 }
            );
            let b = open(d.path(), "b", &id.space).await;
            b.sync(&s).await.unwrap();
            let state = b.snapshot().await.unwrap();
            if count == 0 {
                assert!(state.bookmarks.is_empty());
            } else {
                assert_eq!(state.bookmarks[0].title, (count - 1).to_string());
            }
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn inconsistent_dominated_snapshot_is_rejected_atomically() {
    block_on(async {
        use sha2::{Digest, Sha256};
        let d = tempfile::tempdir().unwrap();
        let id = Arc::new(Identity::create_space().unwrap());
        let s = Meter::new();
        let source = Meter::new();
        let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
        let producer = Replica::open(d.path().join("producer"), id).await.unwrap();
        a.commit(vec![edit(1, "must survive")]).await.unwrap();
        a.sync(&s).await.unwrap();
        producer
            .commit(vec![edit(1, "must survive")])
            .await
            .unwrap();
        producer.compact(&source).await.unwrap();
        let objects = source.inner.objects(&directory(&producer));
        let (name, mb) = objects
            .iter()
            .find(|(n, _)| n.starts_with("snap-") && !n.contains(".part-"))
            .unwrap();
        let (pn, pb) = objects.iter().find(|(n, _)| n.contains(".part-")).unwrap();
        let body = String::from_utf8(pb.clone()).unwrap();
        let at = body.find(",\"objects\":").unwrap();
        // Correct hashes, canonical encoding, same heads, but omit a still-live candidate.
        let wrong = format!("{},\"objects\":{{}}}}", &body[..at]);
        let hash: String = Sha256::digest(wrong.as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let old = name.trim_start_matches("snap-").trim_end_matches(".ds5");
        let manifest = String::from_utf8(mb.clone())
            .unwrap()
            .replace(old, &hash)
            .replace(
                &format!("\"bytes\":{}", body.len()),
                &format!("\"bytes\":{}", wrong.len()),
            );
        a.commit(vec![edit(2, "also survives")]).await.unwrap();
        a.sync(&s).await.unwrap();
        let before = a.snapshot().await.unwrap();
        let dir = directory(&a);
        s.inner
            .put(&dir, &pn.replace(old, &hash), wrong.into_bytes())
            .await
            .unwrap();
        s.inner
            .put(&dir, &name.replace(old, &hash), manifest.into_bytes())
            .await
            .unwrap();
        assert!(a.sync(&s).await.is_err());
        assert_eq!(a.snapshot().await.unwrap(), before);
        a.close().await.unwrap();
        producer.close().await.unwrap();
    });
}

#[test]
fn incomplete_snapshot_can_be_superseded_by_records_without_granting_deletion_authority() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &identity.space).await;
        a.commit(vec![edit(1, "older")]).await.unwrap();
        a.sync(&s).await.unwrap();
        a.compact_with_options(
            &s,
            SyncOptions {
                max_deletions: 0,
                ..SyncOptions::default()
            },
        )
        .await
        .unwrap();
        let dir = directory(&a);
        let old = s
            .inner
            .objects(&dir)
            .keys()
            .find(|n| n.starts_with("snap-") && !n.contains(".part-"))
            .unwrap()
            .clone();
        s.inner.lose(&dir, &part(&s, &dir));
        a.commit(vec![edit(1, "successor")]).await.unwrap();
        for o in a.local_objects().await.unwrap() {
            s.put(&dir, &o.filename, o.bytes).await.unwrap();
        }
        let b = open(d.path(), "b", &identity.space).await;
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "successor");
        b.compact(&s).await.unwrap();
        assert!(
            s.inner.objects(&dir).contains_key(&old),
            "unverified old header is not deletion authority"
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn missing_unapplied_predecessor_is_never_invented_by_repair() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &identity.space).await;
        let b = open(d.path(), "b", &identity.space).await;
        a.commit(vec![edit(1, "one")]).await.unwrap();
        a.commit(vec![edit(1, "two")]).await.unwrap();
        let records = a.local_objects().await.unwrap();
        let dir = directory(&a);
        s.ensure_directory(&dir).await.unwrap();
        s.put(&dir, &records[1].filename, records[1].bytes.clone())
            .await
            .unwrap();
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().waiting, 1);
        assert!(b.snapshot().await.unwrap().frontier.is_empty());
        s.inner.lose(&dir, &records[1].filename);
        for _ in 0..2 {
            assert!(matches!(b.sync(&s).await, Err(Error::RemoteHistoryMissing)));
            let snapshot = b.snapshot().await.unwrap();
            assert_eq!(snapshot.waiting, 1);
            assert!(snapshot.frontier.is_empty());
        }
        for o in records {
            s.put(&dir, &o.filename, o.bytes).await.unwrap();
        }
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "two");
        assert_eq!(b.snapshot().await.unwrap().waiting, 0);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn zero_deletion_budget_and_freeze_preserve_pending_cleanup_and_report_unrepaired_loss() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &identity.space).await;
        a.commit(vec![edit(1, "durable")]).await.unwrap();
        a.sync(&s).await.unwrap();
        let options = SyncOptions {
            max_deletions: 0,
            ..SyncOptions::default()
        };
        let r = a.compact_with_options(&s, options).await.unwrap();
        assert_eq!(r.pending_deletions, 1);
        assert_eq!(r.deletion_requests, 0);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        a.freeze(&uuid::Uuid::new_v4().to_string()).await.unwrap();
        s.take();
        let r = a.sync(&s).await.unwrap();
        assert_eq!(r.pending_deletions, 1);
        assert_eq!(r.deletion_requests, 0);
        assert_eq!(s.take().ops.get("delete"), None);
        s.inner.lose(&directory(&a), &part(&s, &directory(&a)));
        assert!(matches!(a.sync(&s).await, Err(Error::RemoteHistoryMissing)));
        assert_eq!(count_records(&s, &directory(&a)), 1);
        a.close().await.unwrap();
    });
}

#[test]
fn deletion_batch_receipt_survives_restart_without_repeating_completed_deletes() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Arc::new(Identity::create_space().unwrap());
        let s = Meter::new();
        let armed = Arc::new(AtomicBool::new(false));
        let flag = armed.clone();
        let a = Replica::open_with_probe(
            d.path().join("a"),
            identity.clone(),
            Arc::new(move |p| {
                if p == Checkpoint::SnapshotDeletionBatch && flag.swap(false, Ordering::SeqCst) {
                    Err(day_persistence::DbError::driver("synthetic batch crash"))
                } else {
                    Ok(())
                }
            }),
        )
        .await
        .unwrap();
        for n in 1..=63 {
            a.commit(vec![edit(n, "batch")]).await.unwrap();
        }
        a.sync(&s).await.unwrap();
        armed.store(true, Ordering::SeqCst);
        assert!(a.compact(&s).await.is_err());
        assert_eq!(count_records(&s, &directory(&a)), 31);
        a.close().await.unwrap();
        drop(a);
        let a = Replica::open(d.path().join("a"), identity.clone())
            .await
            .unwrap();
        s.take();
        let r = a.sync(&s).await.unwrap();
        assert_eq!(r.deletion_requests, 31);
        assert_eq!(r.pending_deletions, 0);
        assert_eq!(s.take().ops.get("delete"), Some(&31));
        let b = open(d.path(), "b", &identity.space).await;
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 63);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn a_large_incoming_backlog_defers_downloads_while_cleanup_is_bounded() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &identity.space).await;
        for n in 0..8500 {
            a.commit(vec![edit(1, &n.to_string())]).await.unwrap();
        }
        let dir = directory(&a);
        s.ensure_directory(&dir).await.unwrap();
        for o in a.local_objects().await.unwrap() {
            s.put(&dir, &o.filename, o.bytes).await.unwrap();
        }
        let b = open(d.path(), "b", &identity.space).await;
        let r = b.sync(&s).await.unwrap();
        assert!(r.downloads_deferred);
        assert_eq!(r.downloaded, 8192);
        assert_eq!(r.deletion_requests, 128);
        let first = b.snapshot().await.unwrap();
        assert_eq!(first.transactions, 4096);
        assert_eq!(first.bookmarks[0].title, "8191");
        for round in 0..80 {
            let r = b.sync(&s).await.unwrap();
            assert!(r.deletion_requests <= 128);
            assert!(b.snapshot().await.unwrap().transactions <= 8500);
            if r.pending_deletions == 0
                && !r.downloads_deferred
                && b.snapshot().await.unwrap().transactions < 64
            {
                break;
            }
            assert!(round < 79, "bounded maintenance failed to make progress");
        }
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "8499");
        let c = open(d.path(), "c", &identity.space).await;
        c.sync(&s).await.unwrap();
        assert_eq!(
            c.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
        c.close().await.unwrap();
    });
}

#[test]
fn fresh_reader_skips_covered_records_while_another_writer_finishes_cleanup() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &identity.space).await;
        for n in 1..=63 {
            a.commit(vec![edit(n, "synthetic pending cleanup")])
                .await
                .unwrap();
        }
        a.sync(&s).await.unwrap();
        let r = a
            .compact_with_options(
                &s,
                SyncOptions {
                    max_deletions: 0,
                    ..SyncOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.pending_deletions, 63);
        let b = open(d.path(), "b", &identity.space).await;
        b.sync(&s).await.unwrap();
        s.take();
        for _ in 0..3 {
            b.sync(&s).await.unwrap();
            let metrics = s.take();
            assert_eq!(metrics.ops.get("get"), Some(&2));
            assert_eq!(metrics.read, 116);
        }
        b.compact(&s).await.unwrap();
        assert_eq!(
            count_records(&s, &directory(&a)),
            63,
            "snapshot coverage must not authorize deleting raw inputs"
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn pending_input_receipts_preserve_fork_evidence_before_cleanup() {
    block_on(async {
        for snapshot in [false, true] {
            for own_actor in [false, true] {
                let d = tempfile::tempdir().unwrap();
                let identity = Arc::new(Identity::create_space().unwrap());
                let s = Meter::new();
                let isolated = Meter::new();
                let a = Replica::open(d.path().join("a"), identity.clone())
                    .await
                    .unwrap();
                let clone = Replica::open(d.path().join("clone"), identity.clone())
                    .await
                    .unwrap();
                a.commit(vec![edit(1, "original first event")])
                    .await
                    .unwrap();
                a.commit(vec![edit(1, "current state")]).await.unwrap();
                a.sync(&s).await.unwrap();
                let b = open(d.path(), "observer", &identity.space).await;
                b.sync(&s).await.unwrap();
                let observer = if own_actor { &a } else { &b };
                observer
                    .compact_with_options(
                        &s,
                        SyncOptions {
                            max_deletions: 0,
                            ..SyncOptions::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(observer.snapshot().await.unwrap().transactions, 0);
                clone
                    .commit(vec![edit(1, "conflicting first event")])
                    .await
                    .unwrap();
                let dir = directory(&a);
                if snapshot {
                    clone.compact(&isolated).await.unwrap();
                    for (name, bytes) in isolated.inner.objects(&dir) {
                        if name.starts_with("snap-") {
                            s.put(&dir, &name, bytes).await.unwrap();
                        }
                    }
                } else {
                    let object = clone.local_objects().await.unwrap().remove(0);
                    s.put(&dir, &object.filename, object.bytes).await.unwrap();
                }
                let before = s.inner.objects(&dir);
                assert!(
                    matches!(observer.sync(&s).await, Err(Error::Fork)),
                    "snapshot={snapshot} own_actor={own_actor}"
                );
                assert!(observer.snapshot().await.unwrap().poisoned);
                assert_eq!(s.inner.objects(&dir), before);
                a.close().await.unwrap();
                b.close().await.unwrap();
                clone.close().await.unwrap();
            }
        }
    });
}

#[test]
fn earlier_v5_journals_upgrade_without_trusting_an_old_verification_flag() {
    block_on(async {
        for point in [
            Checkpoint::SnapshotPartWritten,
            Checkpoint::SnapshotManifestWritten,
            Checkpoint::SnapshotVerified,
        ] {
            let d = tempfile::tempdir().unwrap();
            let identity = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            let armed = Arc::new(AtomicBool::new(false));
            let flag = armed.clone();
            let a = Replica::open_with_probe(
                d.path().join("a"),
                identity.clone(),
                Arc::new(move |p| {
                    if p == point && flag.swap(false, Ordering::SeqCst) {
                        Err(day_persistence::DbError::driver(
                            "synthetic old journal interruption",
                        ))
                    } else {
                        Ok(())
                    }
                }),
            )
            .await
            .unwrap();
            a.commit(vec![edit(1, "saved body")]).await.unwrap();
            a.sync(&s).await.unwrap();
            armed.store(true, Ordering::SeqCst);
            assert!(a.compact(&s).await.is_err());
            let dir = directory(&a);
            s.inner.lose(&dir, &part(&s, &dir));
            a.close().await.unwrap();
            drop(a);
            // Emulate the earlier experimental v5 schema: no audit/cursor tables, and no
            // replacement witness inserted until finalization. Keep the original journal.
            let db = rusqlite::Connection::open(d.path().join("a")).unwrap();
            db.execute_batch("DROP TABLE _day_sync_snapshot_checks; DROP TABLE _day_sync_maintenance; DROP TABLE _day_sync_compaction_progress; DELETE FROM _day_sync_snapshots;").unwrap();
            drop(db);
            let a = Replica::open(d.path().join("a"), identity.clone())
                .await
                .unwrap();
            a.commit(vec![edit(2, "after upgrade")]).await.unwrap();
            a.sync(&s).await.unwrap();
            let b = open(d.path(), "b", &identity.space).await;
            b.sync(&s).await.unwrap();
            assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 2, "{point:?}");
            assert_eq!(
                a.snapshot().await.unwrap().candidates,
                b.snapshot().await.unwrap().candidates
            );
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn incremental_outbox_preserves_snapshot_headroom_at_the_threshold() {
    block_on(async {
        for extra in [1, 2, 1000] {
            let d = tempfile::tempdir().unwrap();
            let id = Identity::create_space().unwrap();
            let s = Meter::new();
            s.limit.store(68, Ordering::SeqCst);
            let a = open(d.path(), "a", &id.space).await;
            for i in 0..63 {
                a.commit(vec![edit(1, &i.to_string())]).await.unwrap();
            }
            assert_eq!(a.sync(&s).await.unwrap().uploaded, 63);
            for i in 0..extra {
                a.commit(vec![edit(1, &format!("new-{i}"))]).await.unwrap();
            }
            let report = a.sync(&s).await.unwrap();
            assert_eq!(report.uploaded, 0);
            assert_eq!(report.objects_reclaimed, 63);
            assert_eq!(count_records(&s, &directory(&a)), 0);
            let b = open(d.path(), "b", &id.space).await;
            b.sync(&s).await.unwrap();
            assert_eq!(
                b.snapshot().await.unwrap().bookmarks[0].title,
                format!("new-{}", extra - 1)
            );
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn outbox_receipts_and_partial_objects_survive_every_upload_boundary() {
    block_on(async {
        for point in [
            Checkpoint::BeforeUpload,
            Checkpoint::Uploaded,
            Checkpoint::Verified,
            Checkpoint::BeforePublishReceipt,
            Checkpoint::Published,
        ] {
            for partial in [false, true] {
                let d = tempfile::tempdir().unwrap();
                let id = Arc::new(Identity::create_space().unwrap());
                let s = Meter::new();
                let armed = Arc::new(AtomicBool::new(true));
                let flag = armed.clone();
                let a = Replica::open_with_probe(
                    d.path().join("a"),
                    id.clone(),
                    Arc::new(move |p| {
                        if p == point && flag.swap(false, Ordering::SeqCst) {
                            Err(day_persistence::DbError::driver(
                                "synthetic upload interruption",
                            ))
                        } else {
                            Ok(())
                        }
                    }),
                )
                .await
                .unwrap();
                a.commit(vec![edit(1, "old")]).await.unwrap();
                let raw = a.local_objects().await.unwrap().pop().unwrap();
                assert!(a.sync(&s).await.is_err());
                let dir = directory(&a);
                // Simulate a provider leaving partial bytes, including after a lost receipt.
                if partial {
                    s.inner.put(&dir, &raw.filename, vec![b'x']).await.unwrap();
                }
                a.close().await.unwrap();
                drop(a);
                let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
                for _ in 0..64 {
                    a.commit(vec![edit(1, "latest")]).await.unwrap();
                }
                let report = a.sync(&s).await.unwrap();
                assert!(!report.compaction_capacity_limited);
                assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
                assert_eq!(count_records(&s, &dir), 0, "{point:?}, partial={partial}");
                let b = open(d.path(), "b", &id.space).await;
                b.sync(&s).await.unwrap();
                assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "latest");
                a.close().await.unwrap();
                b.close().await.unwrap();
            }
        }
    });
}

#[test]
fn remote_limits_are_errors_not_local_capacity_fallbacks() {
    block_on(async {
        for explicit in [false, true] {
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            for _ in 0..63 {
                a.commit(vec![edit(1, "old")]).await.unwrap();
            }
            a.sync(&s).await.unwrap();
            a.commit(vec![edit(1, "latest")]).await.unwrap();
            s.oversized_manifest.store(true, Ordering::SeqCst);
            let r = if explicit {
                a.compact(&s).await
            } else {
                a.sync(&s).await
            };
            assert!(matches!(r, Err(Error::Limit)), "{r:?}");
            assert_eq!(count_records(&s, &directory(&a)), 63);
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1);
            a.close().await.unwrap();
            drop(a);
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            assert!(matches!(a.sync(&s).await, Err(Error::Limit)));
            assert_eq!(count_records(&s, &directory(&a)), 63);
            s.oversized_manifest.store(false, Ordering::SeqCst);
            a.sync(&s).await.unwrap();
            assert_eq!(count_records(&s, &directory(&a)), 0);
            let b = open(d.path(), "b", &id.space).await;
            b.sync(&s).await.unwrap();
            assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "latest");
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn unchanged_compaction_audits_without_republication_but_repairs_corruption() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let id = Arc::new(Identity::create_space().unwrap());
        let s = Meter::new();
        let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
        a.commit(vec![edit(1, "stable")]).await.unwrap();
        a.compact(&s).await.unwrap();
        a.close().await.unwrap();
        drop(a);
        let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
        s.take();
        let r = a.compact(&s).await.unwrap();
        let stats = s.take();
        assert_eq!(r.snapshots_verified, 1);
        assert_eq!(r.snapshots_published, 0);
        assert_eq!(stats.written, 0);
        assert_eq!(stats.ops.get("get"), Some(&4));
        assert_eq!(stats.ops.get("list"), Some(&1));
        let dir = directory(&a);
        s.inner
            .put(&dir, &part(&s, &dir), vec![b'x'])
            .await
            .unwrap();
        let repaired = a.compact(&s).await.unwrap();
        assert!(repaired.repaired_history);
        assert_eq!(repaired.snapshots_published, 1);
        let b = open(d.path(), "b", &id.space).await;
        b.sync(&s).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "stable");
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn resumed_snapshot_defers_a_large_new_outbox_without_filling_quota() {
    block_on(async {
        for point in [Checkpoint::SnapshotPrepared, Checkpoint::SnapshotVerified] {
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            s.limit.store(68, Ordering::SeqCst);
            let armed = Arc::new(AtomicBool::new(true));
            let flag = armed.clone();
            let a = Replica::open_with_probe(
                d.path().join("a"),
                id.clone(),
                Arc::new(move |p| {
                    if p == point && flag.swap(false, Ordering::SeqCst) {
                        Err(day_persistence::DbError::driver(
                            "synthetic snapshot interruption",
                        ))
                    } else {
                        Ok(())
                    }
                }),
            )
            .await
            .unwrap();
            for _ in 0..65 {
                a.commit(vec![edit(1, "old")]).await.unwrap();
            }
            assert!(a.sync(&s).await.is_err());
            a.close().await.unwrap();
            drop(a);
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            for _ in 0..1000 {
                a.commit(vec![edit(1, "latest")]).await.unwrap();
            }
            let report = a.sync(&s).await.unwrap();
            assert!(report.uploads_deferred);
            assert_eq!(report.uploaded, 0);
            assert_eq!(report.snapshots_published, 1);
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1000);
            assert_eq!(count_records(&s, &directory(&a)), 0);
            let report = a.sync(&s).await.unwrap();
            assert!(!report.uploads_deferred);
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
            let b = open(d.path(), "b", &id.space).await;
            b.sync(&s).await.unwrap();
            assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "latest");
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn insufficient_snapshot_headroom_preserves_the_outbox_until_storage_returns() {
    block_on(async {
        for limit in [2, 3, 4] {
            let d = tempfile::tempdir().unwrap();
            let id = Arc::new(Identity::create_space().unwrap());
            let s = Meter::new();
            s.limit.store(limit, Ordering::SeqCst);
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            for _ in 0..65 {
                a.commit(vec![edit(1, "retained locally")]).await.unwrap();
            }
            for _ in 0..2 {
                let r = a.sync(&s).await;
                if limit < 4 {
                    assert!(
                        matches!(r, Err(Error::Store(ref e)) if e.kind == StoreErrorKind::InsufficientStorage),
                        "{r:?}"
                    );
                    assert_eq!(a.snapshot().await.unwrap().pending_uploads, 65);
                } else {
                    assert!(r.is_ok(), "{r:?}");
                }
                assert_eq!(count_records(&s, &directory(&a)), 0);
            }
            a.close().await.unwrap();
            drop(a);
            s.limit.store(68, Ordering::SeqCst);
            let a = Replica::open(d.path().join("a"), id.clone()).await.unwrap();
            a.sync(&s).await.unwrap();
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
            let b = open(d.path(), "b", &id.space).await;
            b.sync(&s).await.unwrap();
            assert_eq!(
                b.snapshot().await.unwrap().bookmarks[0].title,
                "retained locally"
            );
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}

#[test]
fn disabling_cleanup_does_not_starve_a_new_outbox_behind_an_older_journal() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let id = Identity::create_space().unwrap();
        let s = Meter::new();
        let a = open(d.path(), "a", &id.space).await;
        a.commit(vec![edit(1, "old")]).await.unwrap();
        a.sync(&s).await.unwrap();
        let options = SyncOptions {
            max_deletions: 0,
            ..SyncOptions::default()
        };
        assert_eq!(
            a.compact_with_options(&s, options)
                .await
                .unwrap()
                .pending_deletions,
            1
        );
        for _ in 0..65 {
            a.commit(vec![edit(1, "latest")]).await.unwrap();
        }
        let report = a.sync_with_options(&s, options).await.unwrap();
        assert!(!report.uploads_deferred);
        assert_eq!(report.uploaded, 65);
        assert_eq!(report.deletion_requests, 0);
        assert_eq!(report.pending_deletions, 1);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        let b = open(d.path(), "b", &id.space).await;
        b.sync_with_options(&s, options).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "latest");
        a.sync(&s).await.unwrap();
        a.sync(&s).await.unwrap();
        assert_eq!(count_records(&s, &directory(&a)), 0);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
