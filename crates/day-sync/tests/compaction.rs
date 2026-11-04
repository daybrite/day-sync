// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::block_on;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn edit(object: &str, value: &str) -> Change {
    Change {
        id: object.into(),
        value: Some(BookmarkValue {
            url: "synthetic://compaction".into(),
            title: value.into(),
        }),
    }
}
async fn open(dir: &std::path::Path, name: &str, space: &Space) -> Replica {
    Replica::open(
        dir.join(name),
        Arc::new(Identity::join(space.clone()).unwrap()),
    )
    .await
    .unwrap()
}
fn active(r: &Replica) -> Vec<String> {
    vec![format!("day-sync-v5-{}", r.space())]
}
#[test]
fn snapshot_only_bootstrap_offline_backlog_and_repeated_compaction() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        let b = open(d.path(), "b", &space).await;
        let x = id();
        a.commit(vec![edit(&x, "base")]).await.unwrap();
        a.sync(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        b.commit(vec![edit(&x, "offline")]).await.unwrap();
        a.commit(vec![Change {
            id: x.clone(),
            value: None,
        }])
        .await
        .unwrap();
        a.sync(&store).await.unwrap();
        a.compact(&store).await.unwrap();
        let before = store.objects(&active(&a));
        assert!(!before.keys().any(|n| n.starts_with("tx-")));
        assert_eq!(a.snapshot().await.unwrap().transactions, 0);
        a.compact(&store).await.unwrap();
        assert_eq!(before, store.objects(&active(&a)));
        b.sync(&store).await.unwrap();
        a.sync(&store).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().candidates.len(), 2);
        assert!(a.snapshot().await.unwrap().bookmarks.is_empty());
        b.compact(&store).await.unwrap();
        let c = open(d.path(), "c", &space).await;
        c.sync(&store).await.unwrap();
        assert_eq!(
            c.snapshot().await.unwrap().candidates,
            a.snapshot().await.unwrap().candidates
        );
        c.commit(vec![edit(&x, "resurrect")]).await.unwrap();
        c.sync(&store).await.unwrap();
        a.sync(&store).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().bookmarks[0].title, "resurrect");
        a.close().await.unwrap();
        b.close().await.unwrap();
        c.close().await.unwrap();
    });
}
#[test]
fn every_compaction_boundary_recovers_after_reopen_with_new_edits() {
    block_on(async {
        for point in [
            Checkpoint::SnapshotPrepared,
            Checkpoint::SnapshotPartWritten,
            Checkpoint::SnapshotManifestWritten,
            Checkpoint::SnapshotVerified,
            Checkpoint::SnapshotInputDeleted,
            Checkpoint::SnapshotDeletionBatch,
            Checkpoint::SnapshotFinalized,
        ] {
            let d = tempfile::tempdir().unwrap();
            let identity = Arc::new(Identity::create_space().unwrap());
            let store = MemoryStore::new();
            let armed = Arc::new(AtomicBool::new(false));
            let flag = armed.clone();
            let a = Replica::open_with_probe(
                d.path().join("a"),
                identity.clone(),
                Arc::new(move |p| {
                    if p == point && flag.swap(false, Ordering::SeqCst) {
                        Err(day_persistence::DbError::driver("synthetic crash"))
                    } else {
                        Ok(())
                    }
                }),
            )
            .await
            .unwrap();
            let x = id();
            a.commit(vec![edit(&x, "before")]).await.unwrap();
            a.sync(&store).await.unwrap();
            armed.store(true, Ordering::SeqCst);
            assert!(a.compact(&store).await.is_err(), "{point:?}");
            a.close().await.unwrap();
            drop(a);
            let a = Replica::open(d.path().join("a"), identity.clone())
                .await
                .unwrap();
            a.commit(vec![edit(&x, "after")]).await.unwrap();
            a.compact(&store).await.unwrap();
            a.sync(&store).await.unwrap();
            a.compact(&store).await.unwrap();
            let b = open(d.path(), "b", &identity.space).await;
            b.sync(&store).await.unwrap();
            assert_eq!(
                b.snapshot().await.unwrap().bookmarks[0].title,
                "after",
                "{point:?}"
            );
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
fn snapshot_detects_restore_before_upload_and_frozen_space_never_deletes() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let identity = Arc::new(Identity::create_space().unwrap());
        let store = MemoryStore::new();
        let a = Replica::open(d.path().join("a"), identity.clone())
            .await
            .unwrap();
        let x = id();
        a.commit(vec![edit(&x, "base")]).await.unwrap();
        a.sync(&store).await.unwrap();
        a.close().await.unwrap();
        drop(a);
        std::fs::copy(d.path().join("a"), d.path().join("clone")).unwrap();
        let a = Replica::open(d.path().join("a"), identity.clone())
            .await
            .unwrap();
        a.commit(vec![edit(&x, "new")]).await.unwrap();
        a.sync(&store).await.unwrap();
        a.compact(&store).await.unwrap();
        let copy = Replica::open(d.path().join("clone"), identity)
            .await
            .unwrap();
        let before = store.objects(&active(&a));
        assert!(matches!(copy.sync(&store).await, Err(Error::Fork)));
        assert_eq!(store.objects(&active(&a)), before);
        a.freeze(&id()).await.unwrap();
        assert!(a.compact(&store).await.is_err());
        assert_eq!(store.objects(&active(&a)), before);
        a.close().await.unwrap();
        copy.close().await.unwrap();
    });
}
#[test]
fn multipart_snapshot_keeps_large_values_and_unpublished_suffix() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        let b = open(d.path(), "b", &space).await;
        a.commit((0..600).map(|_| edit(&id(), &"é".repeat(4000))).collect())
            .await
            .unwrap();
        a.sync(&store).await.unwrap();
        a.compact(&store).await.unwrap();
        assert!(
            store
                .objects(&active(&a))
                .keys()
                .filter(|n| n.contains(".part-"))
                .count()
                >= 2
        );
        let local = id();
        b.commit(vec![edit(&local, "pending")]).await.unwrap();
        let (part, bytes) = store
            .objects(&active(&a))
            .into_iter()
            .find(|(n, _)| n.contains(".part-"))
            .unwrap();
        store.lose(&active(&a), &part);
        assert!(matches!(
            b.sync(&store).await,
            Err(Error::RemoteHistoryMissing)
        ));
        assert_eq!(b.snapshot().await.unwrap().candidates.len(), 1);
        store.put(&active(&a), &part, bytes).await.unwrap();
        b.sync(&store).await.unwrap();
        a.sync(&store).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().bookmarks.len(), 601);
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn lost_or_corrupt_known_snapshot_repairs_without_new_edits() {
    block_on(async {
        for part in [false, true] {
            for corrupt in [false, true] {
                let d = tempfile::tempdir().unwrap();
                let space = Identity::create_space().unwrap().space;
                let store = MemoryStore::new();
                let a = open(d.path(), "a", &space).await;
                a.commit(vec![edit(&id(), "survives")]).await.unwrap();
                a.sync(&store).await.unwrap();
                a.compact(&store).await.unwrap();
                let name = store
                    .objects(&active(&a))
                    .keys()
                    .find(|n| n.starts_with("snap-") && n.contains(".part-") == part)
                    .unwrap()
                    .clone();
                if corrupt {
                    store.corrupt(&active(&a), &name, b"synthetic corruption".to_vec());
                } else {
                    store.lose(&active(&a), &name);
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
                let b = open(d.path(), "b", &space).await;
                b.sync(&store).await.unwrap();
                assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "survives");
                a.close().await.unwrap();
                b.close().await.unwrap();
            }
        }
    });
}

#[test]
fn compacted_epoch_handoff_and_genesis_successor_preserve_state() {
    block_on(async {
        use day_sync::lifecycle::{Package, RecoveryPolicy};
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        let x = id();
        a.commit(vec![edit(&x, "baseline")]).await.unwrap();
        a.sync(&store).await.unwrap();
        a.compact(&store).await.unwrap();
        a.freeze(&id()).await.unwrap();
        a.sync(&store).await.unwrap();
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        let package = a
            .prepare_checkpoint(&target, vec![a.attest().await.unwrap()], vec![])
            .await
            .unwrap();
        let pin = package.pin().unwrap();
        let backup = Package::verify_backup(&package.export().unwrap(), &pin).unwrap();
        package.publish(&store, &pin).await.unwrap();
        assert!(
            a.collect_history(&store, &package, &pin, &backup)
                .await
                .unwrap()
                >= 2
        );
        assert!(
            !store
                .objects(&active(&a))
                .keys()
                .any(|n| n.starts_with("snap-"))
        );
        target.sync(&store).await.unwrap();
        target.commit(vec![edit(&x, "successor")]).await.unwrap();
        assert_eq!(target.snapshot().await.unwrap().frontier[target.actor()], 2);
        target.sync(&store).await.unwrap();
        target.compact(&store).await.unwrap();
        target.sync(&store).await.unwrap();
        assert!(
            !store
                .objects(&active(&target))
                .keys()
                .any(|n| n.starts_with("genesis-")),
            "old genesis must not be republished after compaction"
        );
        let b = open(d.path(), "b", &target.space_descriptor()).await;
        b.sync(&store).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "successor");
        let recovery = package
            .recover(&pin, RecoveryPolicy::AcceptLossAfterCheckpoint)
            .unwrap();
        let r = Replica::open(d.path().join("recovery"), recovery.identity.clone())
            .await
            .unwrap();
        recovery.seed(&r).await.unwrap();
        r.sync(&store).await.unwrap();
        assert_eq!(r.snapshot().await.unwrap().bookmarks[0].title, "baseline");
        assert_eq!(r.snapshot().await.unwrap().frontier.len(), 1);
        for r in [&a, &target, &b, &r] {
            r.close().await.unwrap();
        }
    });
}

#[test]
fn more_than_4096_commits_no_longer_exhaust_the_space() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        let x = id();
        for i in 0..4_200 {
            a.commit(vec![edit(&x, &i.to_string())]).await.unwrap();
        }
        a.sync(&store).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().transactions, 0);
        let b = open(d.path(), "b", &space).await;
        b.sync(&store).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "4199");
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn checkpoint_genesis_exceeds_the_old_65536_object_limit() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        for range in [1..40_001u128, 40_001..65_538] {
            a.commit(
                range
                    .map(|n| edit(&uuid::Uuid::from_u128(n).to_string(), "synthetic baseline"))
                    .collect(),
            )
            .await
            .unwrap();
        }
        a.sync(&store).await.unwrap();
        a.freeze(&id()).await.unwrap();
        a.sync(&store).await.unwrap();
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        let package = a
            .prepare_checkpoint(&target, vec![a.attest().await.unwrap()], vec![])
            .await
            .unwrap();
        let pin = package.pin().unwrap();
        let b = open(d.path(), "b", &target.space_descriptor()).await;
        b.install_checkpoint(&package, &pin).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 65_537);
        assert_eq!(b.snapshot().await.unwrap().frontier.len(), 1);
        for r in [&a, &b, &target] {
            r.close().await.unwrap();
        }
    });
}

#[test]
fn oversized_state_keeps_outbox_and_can_compact_after_values_shrink() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(d.path(), "a", &space).await;
        a.sync(&store).await.unwrap();
        let ids: Vec<_> = (0..4100).map(|_| id()).collect();
        a.commit(
            ids.iter()
                .map(|id| Change {
                    id: id.clone(),
                    value: Some(BookmarkValue {
                        url: "x".repeat(8192),
                        title: "y".repeat(8192),
                    }),
                })
                .collect(),
        )
        .await
        .unwrap();
        let count = a.local_objects().await.unwrap().len();
        let before = store.objects(&active(&a));
        let failure = a.compact(&store).await.unwrap_err();
        assert!(matches!(failure, Error::Limit), "{failure}");
        assert_eq!(store.objects(&active(&a)), before);
        assert_eq!(a.local_objects().await.unwrap().len(), count);
        let report = a.sync(&store).await.unwrap();
        assert!(report.compaction_capacity_limited);
        assert_eq!(report.uploaded, count);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        a.commit(
            ids.into_iter()
                .map(|id| Change { id, value: None })
                .collect(),
        )
        .await
        .unwrap();
        a.compact(&store).await.unwrap();
        assert!(a.local_objects().await.unwrap().is_empty());
        let b = open(d.path(), "b", &space).await;
        b.sync(&store).await.unwrap();
        assert_eq!(b.snapshot().await.unwrap().candidates.len(), 4100);
        assert!(b.snapshot().await.unwrap().bookmarks.is_empty());
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn sparse_individual_provenance_never_becomes_an_inferred_delete_range() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space().unwrap().space;
        let store = MemoryStore::new();
        let a = open(dir.path(), "a", &space).await;
        let b = open(dir.path(), "b", &space).await;
        let x = id();
        for i in 1..=10 {
            a.commit(vec![edit(&x, &i.to_string())]).await.unwrap();
        }
        a.sync(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        for i in 11..=20 {
            a.commit(vec![edit(&x, &i.to_string())]).await.unwrap();
        }
        a.sync(&store).await.unwrap();
        let old = a.local_objects().await.unwrap();
        a.compact(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        for i in 21..=30 {
            a.commit(vec![edit(&x, &i.to_string())]).await.unwrap();
        }
        a.sync(&store).await.unwrap();
        // A replicated folder resurrects history B only knows through the snapshot.
        for o in old {
            store.put(&active(&a), &o.filename, o.bytes).await.unwrap();
        }
        b.sync(&store).await.unwrap();
        b.compact(&store).await.unwrap();
        let remaining: Vec<u64> = store
            .objects(&active(&a))
            .keys()
            .filter(|n| n.starts_with("tx-"))
            .map(|n| n.split('.').nth(1).unwrap().parse().unwrap())
            .collect();
        assert_eq!(remaining.len(), 10);
        assert!(remaining.iter().all(|n| (11..=20).contains(n)));
        let c = open(dir.path(), "c", &space).await;
        c.sync(&store).await.unwrap();
        assert_eq!(c.snapshot().await.unwrap().bookmarks[0].title, "30");
        for r in [&a, &b, &c] {
            r.close().await.unwrap();
        }
    });
}
