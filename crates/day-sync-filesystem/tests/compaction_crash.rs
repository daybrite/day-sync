// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use day_sync_filesystem::FilesystemStore;
use futures_lite::future::block_on;
use std::{path::PathBuf, sync::Arc};
#[test]
#[ignore = "subprocess entry; the parent supplies a disposable synthetic fixture"]
fn compaction_child() {
    block_on(async {
        let path = PathBuf::from(std::env::var("COMPACTION_FIXTURE").unwrap());
        let point = std::env::var("COMPACTION_POINT").unwrap();
        let identity = Arc::new(
            Identity::from_bytes(&std::fs::read(path.join("identity.json")).unwrap()).unwrap(),
        );
        let r = Replica::open_with_probe(
            path.join("a.db"),
            identity,
            Arc::new(move |p| {
                if format!("{p:?}") == point {
                    std::process::exit(86);
                }
                Ok(())
            }),
        )
        .await
        .unwrap();
        r.compact(&FilesystemStore::open(path.join("store")).unwrap())
            .await
            .unwrap();
        panic!("crash point was not reached");
    });
}
#[test]
fn process_death_at_every_snapshot_boundary_preserves_a_complete_copy() {
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
            std::fs::write(d.path().join("identity.json"), identity.to_bytes().unwrap()).unwrap();
            let store = FilesystemStore::open(d.path().join("store")).unwrap();
            let a = Replica::open(d.path().join("a.db"), identity.clone())
                .await
                .unwrap();
            a.commit(vec![Change {
                id: uuid::Uuid::new_v4().to_string(),
                value: Some(BookmarkValue {
                    url: "synthetic://crash".into(),
                    title: "durable".into(),
                }),
            }])
            .await
            .unwrap();
            a.sync(&store).await.unwrap();
            let expected = a.snapshot().await.unwrap().candidates;
            a.close().await.unwrap();
            drop(a);
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "compaction_child", "--nocapture"])
                .env("COMPACTION_FIXTURE", d.path())
                .env("COMPACTION_POINT", format!("{point:?}"))
                .output()
                .unwrap();
            assert_eq!(
                output.status.code(),
                Some(86),
                "{point:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let a = Replica::open(d.path().join("a.db"), identity.clone())
                .await
                .unwrap();
            a.compact(&store).await.unwrap();
            let b = Replica::open(
                d.path().join("b.db"),
                Arc::new(Identity::join(identity.space.clone()).unwrap()),
            )
            .await
            .unwrap();
            b.sync(&store).await.unwrap();
            assert_eq!(
                b.snapshot().await.unwrap().candidates,
                expected,
                "{point:?}"
            );
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}
