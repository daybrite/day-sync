// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Disposable synthetic scenario: offline edits, bounded cleanup, replacement loss, fresh join.
//! Run with: cargo run --offline -p day-sync --example compaction
use day_sync::*;
use futures_lite::future::block_on;
use std::sync::Arc;

fn main() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let identity = Identity::create_space().unwrap();
        let store = MemoryStore::new();
        let a = Replica::open(directory.path().join("a"), Arc::new(identity.clone()))
            .await
            .unwrap();
        let b = Replica::open(
            directory.path().join("b"),
            Arc::new(Identity::join(identity.space.clone()).unwrap()),
        )
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let edit = |title: &str| Change {
            id: id.clone(),
            value: Some(BookmarkValue {
                url: "synthetic://compaction-example".into(),
                title: title.into(),
            }),
        };
        a.commit(vec![edit("Synthetic baseline")]).await.unwrap();
        a.sync(&store).await.unwrap();
        b.sync(&store).await.unwrap();

        // The phone deletes while offline. The laptop edits and compacts independently.
        b.commit(vec![Change {
            id: id.clone(),
            value: None,
        }])
        .await
        .unwrap();
        a.commit(vec![edit("Synthetic laptop edit")]).await.unwrap();
        a.compact(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        a.sync(&store).await.unwrap();
        let state = a.snapshot().await.unwrap();
        assert_eq!(state.candidates.len(), 2); // both concurrent intentions survive
        assert!(state.bookmarks.is_empty()); // default resolver gives concurrent deletion priority

        let first = a
            .compact_with_options(
                &store,
                SyncOptions {
                    max_deletions: 1,
                    ..SyncOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(first.pending_deletions > 0);
        println!("{first:#?}");
        // Simulate loss after a successful bounded attempt, before the next cleanup attempt.
        let remote = vec![format!("day-sync-v5-{}", a.space())];
        let owner = first.published_snapshot.unwrap();
        let prefix = owner.trim_end_matches(".ds5");
        let part = store
            .objects(&remote)
            .keys()
            .find(|n| n.starts_with(prefix) && n.contains(".part-"))
            .unwrap()
            .clone();
        store.lose(&remote, &part);
        a.close().await.unwrap();
        drop(a);

        let a = Replica::open(directory.path().join("a"), Arc::new(identity.clone()))
            .await
            .unwrap();
        let resumed = a.sync(&store).await.unwrap();
        assert!(resumed.repaired_history);
        assert_eq!(resumed.pending_deletions, 0);
        println!("{resumed:#?}");
        let c = Replica::open(
            directory.path().join("c"),
            Arc::new(Identity::join(identity.space).unwrap()),
        )
        .await
        .unwrap();
        c.sync(&store).await.unwrap();
        assert_eq!(c.snapshot().await.unwrap().candidates, state.candidates);

        // A sizeable offline outbox publishes as one snapshot, without first uploading
        // individual records and then needing extra storage to replace them.
        for i in 0..65 {
            a.commit(vec![edit(&format!("Synthetic offline edit {i}"))])
                .await
                .unwrap();
        }
        let bulk = a.sync(&store).await.unwrap();
        assert_eq!(bulk.uploaded, 0);
        assert_eq!(bulk.snapshots_published, 1);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        c.sync(&store).await.unwrap();
        assert_eq!(
            c.snapshot().await.unwrap().bookmarks[0].title,
            "Synthetic offline edit 64"
        );
        println!("{bulk:#?}");
        let audited = a.compact(&store).await.unwrap();
        assert_eq!(audited.snapshots_verified, 1);
        assert_eq!(audited.snapshots_published, 0);
        for r in [&a, &b, &c] {
            r.close().await.unwrap();
        }
    });
}
