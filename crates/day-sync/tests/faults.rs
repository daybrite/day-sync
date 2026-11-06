// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::{block_on, zip};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};
fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
#[derive(Clone)]
struct Faults {
    inner: MemoryStore,
    pause: Arc<AtomicU8>,
    fault: Arc<Mutex<Option<(char, usize, bool)>>>,
    hidden: Arc<Mutex<BTreeSet<String>>>,
    reads: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}
impl Faults {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            pause: Arc::default(),
            fault: Arc::default(),
            hidden: Arc::default(),
            reads: Arc::default(),
        }
    }
    fn arm(&self, op: char, n: usize, after: bool) {
        *self.fault.lock().unwrap() = Some((op, n, after));
    }
    fn hit(&self, op: char, after: bool) -> Result<(), StoreError> {
        let mut f = self.fault.lock().unwrap();
        if let Some((kind, n, late)) = &mut *f
            && *kind == op
            && *late == after
        {
            if *n == 0 {
                *f = None;
                return Err(StoreError::new(StoreErrorKind::Unavailable));
            }
            *n -= 1;
        }
        Ok(())
    }
    fn witness(&self, d: &[String], name: &str) {
        let reads = self.reads.lock().unwrap();
        assert!(
            reads.contains_key(name),
            "deleted an input never individually read: {name}"
        );
        let mut required = BTreeMap::new();
        let owner = if let Some(rest) = name.strip_prefix("tx-") {
            let fields: Vec<_> = rest.split('.').collect();
            required.insert(fields[0].to_owned(), fields[1].parse::<u64>().unwrap());
            assert_eq!(digest(&reads[name]), fields[2]);
            None
        } else {
            let owner = name
                .split(".part-")
                .next()
                .unwrap()
                .trim_end_matches(".ds5");
            let manifest = &reads[&format!("{owner}.ds5")];
            let m: serde_json::Value = serde_json::from_slice(manifest).unwrap();
            for (a, h) in m["heads"].as_object().unwrap() {
                required.insert(a.clone(), h["sequence"].as_u64().unwrap());
            }
            Some(owner)
        };
        let objects = self.inner.objects(d);
        assert!(
            objects.iter().any(|(n, b)| {
                if !n.starts_with("snap-")
                    || n.contains(".part-")
                    || owner.is_some_and(|o| n == &format!("{o}.ds5"))
                {
                    return false;
                }
                let Ok(m) = serde_json::from_slice::<serde_json::Value>(b) else {
                    return false;
                };
                if required
                    .iter()
                    .any(|(a, seq)| m["heads"][a]["sequence"].as_u64().unwrap_or(0) < *seq)
                {
                    return false;
                }
                let mut body = vec![];
                for (i, p) in m["parts"].as_array().unwrap().iter().enumerate() {
                    let part = format!(
                        "snap-{}.part-{i}.{}.ds5",
                        m["state"].as_str().unwrap(),
                        p["hash"].as_str().unwrap()
                    );
                    let Some(bytes) = objects.get(&part) else {
                        return false;
                    };
                    if bytes.len() != p["bytes"].as_u64().unwrap() as usize
                        || digest(bytes) != p["hash"].as_str().unwrap()
                    {
                        return false;
                    }
                    body.extend_from_slice(bytes);
                }
                let Ok(state) = serde_json::from_slice::<serde_json::Value>(&body) else {
                    return false;
                };
                digest(&body) == m["state"].as_str().unwrap()
                    && body.len() == m["bytes"].as_u64().unwrap() as usize
                    && state["heads"] == m["heads"]
            }),
            "input deleted without a complete distinct covering snapshot: {name}"
        );
    }
}
impl Store for Faults {
    fn endpoint(&self) -> String {
        self.inner.endpoint()
    }
    fn ensure_directory<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, ()> {
        self.inner.ensure_directory(d)
    }
    fn list<'a>(&'a self, d: &'a [String]) -> StoreFuture<'a, Vec<String>> {
        Box::pin(async move {
            self.hit('l', false)?;
            let mut names = self.inner.list(d).await?;
            names.retain(|n| !self.hidden.lock().unwrap().contains(n));
            self.hit('l', true)?;
            Ok(names)
        })
    }
    fn get<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.hit('g', false)?;
            let r = self.inner.get(d, n).await?;
            self.hit('g', true)?;
            if let Some(b) = &r {
                self.reads.lock().unwrap().insert(n.into(), b.clone());
            }
            Ok(r)
        })
    }
    fn put<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.hit('p', false)?;
            self.inner.put(d, n, b).await?;
            self.hit('p', true)
        })
    }
    fn create<'a>(&'a self, d: &'a [String], n: &'a str, b: Vec<u8>) -> StoreFuture<'a, Created> {
        Box::pin(async move {
            self.hit('c', false)?;
            let r = self.inner.create(d, n, b).await?;
            self.hit('c', true)?;
            Ok(r)
        })
    }
    fn delete<'a>(&'a self, d: &'a [String], n: &'a str) -> StoreFuture<'a, bool> {
        Box::pin(async move {
            if self.pause.load(Ordering::SeqCst) != 0 {
                self.pause.store(2, Ordering::SeqCst);
                std::future::pending::<()>().await;
            }
            self.hit('d', false)?;
            self.witness(d, n);
            let r = self.inner.delete(d, n).await?;
            self.hit('d', true)?;
            Ok(r)
        })
    }
}
fn edit(id: &str, n: u64) -> Change {
    Change {
        id: id.into(),
        value: if n.is_multiple_of(5) {
            None
        } else {
            Some(BookmarkValue {
                url: "synthetic://faults".into(),
                title: n.to_string(),
            })
        },
    }
}
async fn open(d: &std::path::Path, n: &str, s: &Space) -> Replica {
    Replica::open(d.join(n), Arc::new(Identity::join(s.clone()).unwrap()))
        .await
        .unwrap()
}
/// The length of a long schedule. Every engine transaction reaches disk (`synchronous=FULL`),
/// and GitHub's Windows runners flush several times slower than Linux: at full length the longer
/// of the two schedules below ran eleven minutes there, a tenth of that on Linux, and pushed the
/// workflow's thirty-minute test step over its limit. Windows keeps each schedule's shape and
/// runs a quarter of its length.
fn scaled(len: u64) -> u64 {
    if cfg!(windows) { len / 4 } else { len }
}
#[test]
fn failures_before_and_after_store_operations_preserve_recoverability() {
    block_on(async {
        for op in ['l', 'g', 'c', 'd'] {
            for after in [false, true] {
                for nth in 0..8 {
                    let dir = tempfile::tempdir().unwrap();
                    let space = Identity::create_space().unwrap().space;
                    let store = Faults::new();
                    let a = open(dir.path(), "a", &space).await;
                    let x = uuid::Uuid::new_v4().to_string();
                    for i in 1..4 {
                        a.commit(vec![edit(&x, i)]).await.unwrap();
                    }
                    a.sync(&store).await.unwrap();
                    store.arm(op, nth, after);
                    let _ = a.compact(&store).await;
                    *store.fault.lock().unwrap() = None;
                    a.compact(&store).await.unwrap();
                    let b = open(dir.path(), "b", &space).await;
                    b.sync(&store).await.unwrap();
                    assert_eq!(
                        a.snapshot().await.unwrap().candidates,
                        b.snapshot().await.unwrap().candidates,
                        "{op} {after} {nth}"
                    );
                    a.close().await.unwrap();
                    b.close().await.unwrap();
                }
            }
        }
    });
}
#[test]
fn concurrent_compactors_with_different_views_and_resurrected_records_converge() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = Faults::new();
        let a = open(dir.path(), "a", &space).await;
        let b = open(dir.path(), "b", &space).await;
        let x = uuid::Uuid::new_v4().to_string();
        a.commit(vec![edit(&x, 1)]).await.unwrap();
        b.commit(vec![edit(&x, 5)]).await.unwrap();
        // Publish both without delivering the other's record, to model endpoint visibility lag.
        let d = vec![format!("day-sync-v5-{}", space.id)];
        store.ensure_directory(&d).await.unwrap();
        let ar = a.local_objects().await.unwrap().remove(0);
        let br = b.local_objects().await.unwrap().remove(0);
        store.put(&d, &ar.filename, ar.bytes.clone()).await.unwrap();
        store.put(&d, &br.filename, br.bytes.clone()).await.unwrap();
        let av = Faults {
            inner: store.inner.clone(),
            pause: Arc::default(),
            fault: Arc::default(),
            hidden: Arc::new(Mutex::new(BTreeSet::from([br.filename.clone()]))),
            reads: Arc::default(),
        };
        let bv = Faults {
            inner: store.inner.clone(),
            pause: Arc::default(),
            fault: Arc::default(),
            hidden: Arc::new(Mutex::new(BTreeSet::from([ar.filename.clone()]))),
            reads: Arc::default(),
        };
        let (ra, rb) = zip(a.compact(&av), b.compact(&bv)).await;
        ra.unwrap();
        rb.unwrap();
        // A folder replication service can resurrect old names. They must not resurrect old values.
        store.put(&d, &ar.filename, ar.bytes).await.unwrap();
        store.put(&d, &br.filename, br.bytes).await.unwrap();
        for _ in 0..3 {
            a.sync(&store).await.unwrap();
            b.sync(&store).await.unwrap();
        }
        a.compact(&store).await.unwrap();
        let c = open(dir.path(), "c", &space).await;
        c.sync(&store).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            c.snapshot().await.unwrap().candidates
        );
        assert!(c.snapshot().await.unwrap().bookmarks.is_empty());
        for r in [&a, &b, &c] {
            r.close().await.unwrap();
        }
    });
}
#[test]
fn randomized_engine_schedules_match_an_uncompacted_replay() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = Faults::new();
        let mut replicas = vec![];
        for i in 0..4 {
            replicas.push(open(dir.path(), &format!("r{i}"), &space).await);
        }
        let objects: Vec<_> = (0..8).map(|_| uuid::Uuid::new_v4().to_string()).collect();
        let mut seed = 913u64;
        let mut history = vec![];
        // A reproducible long schedule; the separate pure-state suite generates 10,000 histories.
        for step in 0..scaled(10_000) {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let r = &replicas[((seed >> 32) % 4) as usize];
            match seed % 5 {
                0 | 1 => {
                    r.commit(vec![edit(&objects[((seed >> 40) % 8) as usize], step)])
                        .await
                        .unwrap();
                    history.push(r.local_objects().await.unwrap().pop().unwrap());
                }
                2 => {
                    r.sync(&store).await.unwrap();
                }
                _ => {
                    if step % 11 == 0 {
                        store.arm('d', 0, step % 2 == 0);
                    }
                    let _ = r.compact(&store).await;
                    *store.fault.lock().unwrap() = None;
                }
            }
        }
        for _ in 0..4 {
            for r in &replicas {
                r.sync(&store).await.unwrap();
            }
        }
        let oracle = open(dir.path(), "oracle", &space).await;
        for o in history {
            oracle.receive(o).await.unwrap();
        }
        oracle.apply_ready().await.unwrap();
        let expected = oracle.snapshot().await.unwrap();
        for r in &replicas {
            let got = r.snapshot().await.unwrap();
            assert_eq!(got.candidates, expected.candidates);
            assert_eq!(got.bookmarks, expected.bookmarks);
            r.close().await.unwrap();
        }
        oracle.close().await.unwrap();
    });
}

#[test]
fn cancellation_releases_admission_but_freeze_prevents_resumed_deletion() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = Faults::new();
        let a = open(dir.path(), "a", &space).await;
        a.commit(vec![edit(&uuid::Uuid::new_v4().to_string(), 1)])
            .await
            .unwrap();
        a.sync(&store).await.unwrap();
        store.pause.store(1, Ordering::SeqCst);
        let mut compact = Box::pin(a.compact(&store));
        while store.pause.load(Ordering::SeqCst) != 2 {
            assert!(
                futures_lite::future::poll_once(&mut compact)
                    .await
                    .is_none()
            );
            futures_lite::future::yield_now().await;
        }
        let token = uuid::Uuid::new_v4().to_string();
        assert!(matches!(a.freeze(&token).await, Err(Error::Busy)));
        drop(compact);
        a.freeze(&token).await.unwrap();
        let directory = vec![format!("day-sync-v5-{}", space.id)];
        let before = store.inner.objects(&directory);
        store.pause.store(0, Ordering::SeqCst);
        assert!(a.compact(&store).await.is_err());
        assert_eq!(before, store.inner.objects(&directory));
        a.close().await.unwrap();
    });
}

#[test]
fn known_snapshot_transport_failure_is_not_mistaken_for_corruption() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = Faults::new();
        let a = open(dir.path(), "a", &space).await;
        a.commit(vec![edit(&uuid::Uuid::new_v4().to_string(), 1)])
            .await
            .unwrap();
        a.sync_with_options(
            &store,
            SyncOptions {
                verify_snapshots: true,
                ..SyncOptions::default()
            },
        )
        .await
        .unwrap();
        a.compact(&store).await.unwrap();
        let directory = vec![format!("day-sync-v5-{}", space.id)];
        let before = store.inner.objects(&directory);
        for offset in [2, 3] {
            // retirement/descriptor first, then known manifest and its part
            store.arm('g', offset, false);
            assert!(matches!(
                a.sync_with_options(
                    &store,
                    SyncOptions {
                        verify_snapshots: true,
                        ..SyncOptions::default()
                    }
                )
                .await,
                Err(Error::Store(_))
            ));
            assert_eq!(before, store.inner.objects(&directory));
        }
        a.sync_with_options(
            &store,
            SyncOptions {
                verify_snapshots: true,
                ..SyncOptions::default()
            },
        )
        .await
        .unwrap();
        a.close().await.unwrap();
    });
}

#[test]
fn seeded_restart_and_transport_schedules_match_original_operation_replay() {
    block_on(async {
        for initial in 0..scaled(32) {
            let d = tempfile::tempdir().unwrap();
            let space = Identity::create_space().unwrap().space;
            let store = Faults::new();
            let identities: Vec<_> = (0..3)
                .map(|_| Arc::new(Identity::join(space.clone()).unwrap()))
                .collect();
            let mut replicas = Vec::new();
            for (i, identity) in identities.iter().enumerate() {
                replicas.push(
                    Replica::open(d.path().join(format!("r{i}")), identity.clone())
                        .await
                        .unwrap(),
                );
            }
            let objects: Vec<_> = (1..=5)
                .map(|n| uuid::Uuid::from_u128(n).to_string())
                .collect();
            let mut seed = initial + 1;
            let mut history = Vec::new();
            for step in 0..128 {
                seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let i = ((seed >> 32) % 3) as usize;
                match seed % 6 {
                    0 | 1 => {
                        replicas[i]
                            .commit(vec![edit(&objects[((seed >> 40) % 5) as usize], step)])
                            .await
                            .unwrap();
                        history.push(replicas[i].local_objects().await.unwrap().pop().unwrap());
                    }
                    2 => {
                        replicas[i].close().await.unwrap();
                        replicas[i] =
                            Replica::open(d.path().join(format!("r{i}")), identities[i].clone())
                                .await
                                .unwrap();
                    }
                    _ => {
                        let op = ['l', 'g', 'c', 'p', 'd'][((seed >> 48) % 5) as usize];
                        store.arm(op, ((seed >> 56) % 8) as usize, seed & (1 << 12) != 0);
                        let result = if seed & 128 != 0 {
                            replicas[i].compact(&store).await
                        } else {
                            replicas[i].sync(&store).await
                        };
                        assert!(
                            result.is_ok() || matches!(result, Err(Error::Store(_))),
                            "seed={initial} step={step} op={op}: {result:?}"
                        );
                        *store.fault.lock().unwrap() = None;
                    }
                }
            }
            for _ in 0..3 {
                for r in &replicas {
                    r.sync(&store).await.unwrap();
                }
            }
            replicas[0].compact(&store).await.unwrap();
            let fresh = open(d.path(), "fresh", &space).await;
            fresh.sync(&store).await.unwrap();
            let oracle = open(d.path(), "oracle", &space).await;
            for object in history {
                oracle.receive(object).await.unwrap();
            }
            oracle.apply_ready().await.unwrap();
            let expected = oracle.snapshot().await.unwrap();
            for r in replicas.iter().chain([&fresh]) {
                let got = r.snapshot().await.unwrap();
                assert_eq!(got.candidates, expected.candidates, "seed={initial}");
                assert_eq!(got.bookmarks, expected.bookmarks, "seed={initial}");
                r.close().await.unwrap();
            }
            oracle.close().await.unwrap();
        }
    });
}
