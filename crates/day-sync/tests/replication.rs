// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::block_on;
mod support;
use std::sync::Arc;
use support::{Fixture, open, open_with_probe};
fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn edit(object: &str, title: &str) -> Change {
    Change {
        id: object.into(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid/".into(),
            title: title.into(),
        }),
    }
}
async fn deliver(from: &Replica, to: &Replica) {
    support::deliver_members(from, to).await;
    for object in from.local_objects().await.unwrap() {
        to.receive(object).await.unwrap();
    }
    to.apply_ready().await.unwrap();
}

#[test]
fn two_databases_conflict_delete_and_explicit_resurrection_converge() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let object = id();
        let a = open(dir.path().join("a.db"), &space).await.unwrap();
        let b = open(dir.path().join("b.db"), &space).await.unwrap();
        a.commit(vec![edit(&object, "first")]).await.unwrap();
        deliver(&a, &b).await;
        a.commit(vec![edit(&object, "alpha")]).await.unwrap();
        b.commit(vec![edit(&object, "beta")]).await.unwrap();
        deliver(&a, &b).await;
        deliver(&b, &a).await;
        let sa = a.snapshot().await.unwrap();
        let sb = b.snapshot().await.unwrap();
        assert_eq!(sa.bookmarks, sb.bookmarks);
        assert_eq!(sa.candidates, sb.candidates);
        assert_eq!(sa.candidates.len(), 2);
        assert_eq!(sa.transactions, 3);
        assert_eq!(sb.transactions, 3);
        a.commit(vec![Change {
            id: object.clone(),
            value: None,
        }])
        .await
        .unwrap();
        b.commit(vec![edit(&object, "concurrent edit")])
            .await
            .unwrap();
        deliver(&a, &b).await;
        deliver(&b, &a).await;
        assert!(a.snapshot().await.unwrap().bookmarks.is_empty());
        assert!(b.snapshot().await.unwrap().bookmarks.is_empty());
        assert_eq!(a.snapshot().await.unwrap().candidates.len(), 2);
        b.commit(vec![edit(&object, "explicit resurrection")])
            .await
            .unwrap();
        deliver(&b, &a).await;
        assert_eq!(
            a.snapshot().await.unwrap().bookmarks[0].title,
            "explicit resurrection"
        );
        assert_eq!(a.snapshot().await.unwrap().candidates.len(), 1);
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn out_of_order_duplicate_and_gap_survive_restart() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let object = id();
        let target = d.path().join("b.db");
        let a = open(d.path().join("a.db"), &space).await.unwrap();
        let b = open(&target, &space).await.unwrap();
        a.commit(vec![edit(&object, "one")]).await.unwrap();
        a.commit(vec![edit(&object, "two")]).await.unwrap();
        let objects = a.local_objects().await.unwrap();
        assert!(b.receive(objects[1].clone()).await.unwrap());
        assert_eq!(b.apply_ready().await.unwrap(), 0);
        assert_eq!(b.snapshot().await.unwrap().waiting, 1);
        assert!(b.snapshot().await.unwrap().bookmarks.is_empty());
        b.close().await.unwrap();
        drop(b);
        let b = open(&target, &space).await.unwrap();
        assert!(!b.receive(objects[1].clone()).await.unwrap());
        b.receive(objects[0].clone()).await.unwrap();
        assert_eq!(b.apply_ready().await.unwrap(), 2);
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "two");
        deliver(&a, &b).await;
        assert_eq!(b.snapshot().await.unwrap().transactions, 2);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn local_journal_failure_rolls_back_model_and_sequence() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let path = d.path().join("a.db");
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = fail.clone();
        let a = open_with_probe(
            &path,
            &space,
            Arc::new(move |point| {
                if point == Checkpoint::AfterJournal
                    && flag.load(std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(day_persistence::DbError::driver(
                        "synthetic journal failure",
                    ));
                }
                Ok(())
            }),
        )
        .await
        .unwrap();
        let change = edit(&id(), "atomic");
        assert!(a.commit(vec![change.clone()]).await.is_err());
        let s = a.snapshot().await.unwrap();
        assert_eq!(s.transactions, 0);
        assert!(s.bookmarks.is_empty());
        assert!(s.candidates.is_empty());
        fail.store(false, std::sync::atomic::Ordering::SeqCst);
        a.commit(vec![change]).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().frontier[a.actor()], 1);
        a.close().await.unwrap();
    });
}
#[test]
fn failed_import_leaves_inbox_and_retries_without_echo() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let object = id();
        let a = open(d.path().join("a.db"), &space).await.unwrap();
        a.commit(vec![edit(&object, "incoming")]).await.unwrap();
        let fail = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = fail.clone();
        let b = open_with_probe(
            d.path().join("b.db"),
            &space,
            Arc::new(move |point| {
                if point == Checkpoint::BeforeApplyCommit
                    && flag.load(std::sync::atomic::Ordering::SeqCst)
                {
                    return Err(day_persistence::DbError::driver("synthetic import failure"));
                }
                Ok(())
            }),
        )
        .await
        .unwrap();
        b.receive(a.local_objects().await.unwrap().remove(0))
            .await
            .unwrap();
        assert!(b.apply_ready().await.is_err());
        let s = b.snapshot().await.unwrap();
        assert!(s.bookmarks.is_empty());
        assert!(s.frontier.is_empty());
        assert_eq!(s.waiting, 1);
        fail.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(b.apply_ready().await.unwrap(), 1);
        assert_eq!(b.snapshot().await.unwrap().pending_uploads, 0);
        assert!(b.local_objects().await.unwrap().is_empty());
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn malformed_cross_space_and_corrupt_objects_never_advance_frontier() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let a = open(d.path().join("a.db"), &space).await.unwrap();
        let b = open(d.path().join("b.db"), &space).await.unwrap();
        a.commit(vec![edit(&id(), "safe")]).await.unwrap();
        let object = a.local_objects().await.unwrap().remove(0);
        let mut bad = object.clone();
        bad.bytes.push(b' ');
        assert!(matches!(b.receive(bad).await, Err(Error::Integrity)));
        let mut bad = object.clone();
        bad.filename = "../escape".into();
        assert!(matches!(b.receive(bad).await, Err(Error::InvalidWire)));
        let c = open(d.path().join("c.db"), &Fixture::new().await)
            .await
            .unwrap();
        assert!(matches!(c.receive(object).await, Err(Error::WrongSpace)));
        assert_eq!(b.snapshot().await.unwrap().transactions, 0);
        assert_eq!(c.snapshot().await.unwrap().transactions, 0);
        a.close().await.unwrap();
        b.close().await.unwrap();
        c.close().await.unwrap();
    });
}
#[test]
fn clone_forks_quarantine_including_a_previously_unseen_own_sequence() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let object = id();
        let path = d.path().join("a.db");
        let copy = d.path().join("clone.db");
        let a = open(&path, &space).await.unwrap();
        a.commit(vec![edit(&object, "base")]).await.unwrap();
        a.close().await.unwrap();
        drop(a);
        std::fs::copy(&path, &copy).unwrap();
        space.copy_identity(&path, &copy);
        let a = open(&path, &space).await.unwrap();
        let b = open(&copy, &space).await.unwrap();
        assert_eq!(a.actor(), b.actor());
        b.commit(vec![edit(&object, "cloned writer")])
            .await
            .unwrap();
        let foreign = b.local_objects().await.unwrap().pop().unwrap();
        assert!(matches!(a.receive(foreign).await, Err(Error::Fork)));
        assert!(a.snapshot().await.unwrap().poisoned);
        assert!(
            a.commit(vec![edit(&object, "must not overwrite")])
                .await
                .is_err()
        );
        a.close().await.unwrap();
        drop(a);
        let a = open(&path, &space).await.unwrap();
        assert!(a.snapshot().await.unwrap().poisoned);
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
#[test]
fn exclusive_open_and_uninstrumented_sql_writes_are_rejected() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("a.db");
        let space = Fixture::new().await;
        let a = open(&path, &space).await.unwrap();
        assert!(matches!(open(&path, &space).await, Err(Error::Busy)));
        let raw = rusqlite::Connection::open(&path).unwrap();
        assert!(
            raw.execute("INSERT INTO sync_bookmarks VALUES(?,'url','title')", [id()])
                .is_err()
        );
        assert_eq!(a.snapshot().await.unwrap().transactions, 0);
        drop(raw);
        a.close().await.unwrap();
    });
}
#[test]
fn local_batch_splits_wire_records_without_splitting_local_commit() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let a = open(d.path().join("a.db"), &Fixture::new().await)
            .await
            .unwrap();
        let edits = (0..40).map(|_| edit(&id(), &"x".repeat(8192))).collect();
        a.commit(edits).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().transactions, 2);
        a.commit(vec![edit(&id(), "one"), edit(&id(), "two")])
            .await
            .unwrap();
        let s = a.snapshot().await.unwrap();
        assert_eq!(s.bookmarks.len(), 42);
        assert_eq!(s.transactions, 3);
        assert_eq!(s.frontier[a.actor()], 3);
        a.close().await.unwrap();
    });
}
