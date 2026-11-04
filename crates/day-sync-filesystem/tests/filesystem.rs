// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A shared folder as the store: store semantics, two apps converging through it, and a
//! checkpoint handoff that never touches a server.
#![cfg(not(target_arch = "wasm32"))]
use day_sync::{lifecycle::Package, *};
use day_sync_filesystem::FilesystemStore;
use futures_lite::future::block_on;
use std::{path::Path, sync::Arc};

fn edit(id: &str, title: &str) -> Change {
    Change {
        id: id.into(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid".into(),
            title: title.into(),
        }),
    }
}

async fn cluster(dir: &Path, n: usize) -> Vec<Replica> {
    let owner = Arc::new(Identity::create_space().unwrap());
    let mut replicas = vec![
        Replica::open(dir.join("0.db"), owner.clone())
            .await
            .unwrap(),
    ];
    for i in 1..n {
        let identity = Arc::new(Identity::join(owner.space.clone()).unwrap());
        replicas.push(
            Replica::open(dir.join(format!("{i}.db")), identity)
                .await
                .unwrap(),
        );
    }
    replicas
}

async fn converge(replicas: &[Replica], store: &dyn Store) {
    for _ in 0..3 {
        for r in replicas {
            r.sync(store).await.unwrap();
        }
    }
    let first = replicas[0].snapshot().await.unwrap();
    for r in replicas {
        let s = r.snapshot().await.unwrap();
        assert_eq!(s.bookmarks, first.bookmarks);
        assert_eq!(s.candidates, first.candidates);
        assert_eq!(s.frontier, first.frontier);
        assert_eq!((s.pending_uploads, s.waiting), (0, 0));
    }
}

#[test]
fn store_semantics_on_a_directory() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStore::open(dir.path().join("share")).unwrap();
        let directory = vec!["space".to_string(), "nested".to_string()];
        assert!(matches!(
            store.list(&directory).await,
            Err(e) if e.kind == StoreErrorKind::NotFound
        ));
        store.ensure_directory(&directory).await.unwrap();
        store.ensure_directory(&directory).await.unwrap();
        assert_eq!(store.list(&directory).await.unwrap(), Vec::<String>::new());
        assert_eq!(
            store.create(&directory, "a", b"1".to_vec()).await.unwrap(),
            Created::Created
        );
        assert_eq!(
            store.create(&directory, "a", b"2".to_vec()).await.unwrap(),
            Created::Exists
        );
        assert_eq!(
            store.get(&directory, "a").await.unwrap(),
            Some(b"1".to_vec())
        );
        store.put(&directory, "a", b"3".to_vec()).await.unwrap();
        assert_eq!(
            store.get(&directory, "a").await.unwrap(),
            Some(b"3".to_vec())
        );
        assert_eq!(store.get(&directory, "missing").await.unwrap(), None);

        // Hidden names are the store's own and never listed or accepted; nothing temporary
        // survives a write.
        std::fs::write(store.root().join("space/nested/.hidden"), b"x").unwrap();
        assert_eq!(store.list(&directory).await.unwrap(), vec!["a".to_string()]);
        assert_eq!(
            store.list(&directory[..1]).await.unwrap(),
            vec!["nested".to_string()]
        );
        for bad in ["", ".", "..", ".tmp", "a/b", "a\\b", "a\0b"] {
            assert!(
                matches!(store.get(&directory, bad).await, Err(e) if e.kind == StoreErrorKind::Invalid),
                "{bad:?}"
            );
        }
        assert!(store.delete(&directory, "a").await.unwrap());
        assert!(!store.delete(&directory, "a").await.unwrap());

        assert!(store.endpoint().starts_with("file://"));
        assert_eq!(
            FilesystemStore::open(dir.path().join("share"))
                .unwrap()
                .endpoint(),
            store.endpoint()
        );
    });
}

#[test]
fn two_apps_synchronize_through_a_shared_folder() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStore::open(dir.path().join("share")).unwrap();
        let replicas = cluster(dir.path(), 2).await;
        let (a, b) = (&replicas[0], &replicas[1]);
        let id = uuid::Uuid::new_v4().to_string();
        a.commit(vec![edit(&id, "from a")]).await.unwrap();
        a.sync(&store).await.unwrap();
        assert_eq!(
            Space::fetch(&store, a.space()).await.unwrap(),
            a.space_descriptor()
        );
        let report = b.sync(&store).await.unwrap();
        assert_eq!((report.downloaded, report.applied), (1, 1));

        // Concurrent edits: two candidates, one winner, identical on both.
        a.commit(vec![edit(&id, "a again")]).await.unwrap();
        b.commit(vec![edit(&id, "b again")]).await.unwrap();
        converge(&replicas, &store).await;
        assert_eq!(a.snapshot().await.unwrap().candidates.len(), 2);

        // A delete propagates and wins over a concurrent edit.
        b.commit(vec![Change {
            id: id.clone(),
            value: None,
        }])
        .await
        .unwrap();
        a.commit(vec![edit(&id, "a once more")]).await.unwrap();
        converge(&replicas, &store).await;
        assert!(a.snapshot().await.unwrap().bookmarks.is_empty());

        // A late joiner replays the whole folder.
        let c = Replica::open(
            dir.path().join("c.db"),
            Arc::new(Identity::join(a.space_descriptor()).unwrap()),
        )
        .await
        .unwrap();
        let report = c.sync(&store).await.unwrap();
        assert_eq!(report.downloaded, 5);
        assert_eq!(
            c.snapshot().await.unwrap().candidates,
            a.snapshot().await.unwrap().candidates
        );

        // The folder is only ever one store; a file that disappears is reported, not applied.
        let other = FilesystemStore::open(dir.path().join("elsewhere")).unwrap();
        assert!(matches!(a.sync(&other).await, Err(Error::WrongEndpoint)));
        let directory = vec![format!("day-sync-v5-{}", a.space())];
        let lost = store
            .list(&directory)
            .await
            .unwrap()
            .into_iter()
            .find(|n| n.starts_with("tx-"))
            .unwrap();
        std::fs::remove_file(store.root().join(&directory[0]).join(&lost)).unwrap();
        assert!(matches!(
            a.sync(&store).await,
            Err(Error::RemoteHistoryMissing)
        ));
        for r in replicas.iter().chain([&c]) {
            r.close().await.unwrap();
        }
    });
}

#[test]
fn checkpoint_handoff_through_a_folder() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = FilesystemStore::open(dir.path().join("share")).unwrap();
        let replicas = cluster(dir.path(), 3).await;
        for (i, r) in replicas.iter().enumerate() {
            r.commit(vec![edit(
                &format!("00000000-0000-4000-8000-{i:012}"),
                &format!("device {i}"),
            )])
            .await
            .unwrap();
        }
        converge(&replicas, &store).await;
        let before = replicas[0].snapshot().await.unwrap().bookmarks;
        assert_eq!(before.len(), 3);

        // Freeze, settle, attest, and hand off into a fresh space.
        let token = uuid::Uuid::new_v4().to_string();
        for r in &replicas {
            r.freeze(&token).await.unwrap();
        }
        converge(&replicas, &store).await;
        let mut receipts = vec![];
        for r in &replicas {
            receipts.push(r.attest().await.unwrap());
        }
        let target = Replica::open(
            dir.path().join("target.db"),
            Arc::new(Identity::create_space_for("day-sync:bookmark:5").unwrap()),
        )
        .await
        .unwrap();
        let package = replicas[0]
            .prepare_checkpoint(&target, receipts, vec![])
            .await
            .unwrap();
        let pin = package.pin().unwrap();
        package.publish(&store, &pin).await.unwrap();

        // The old space is retired for everyone, in the folder and through the descriptor.
        assert!(matches!(
            replicas[1].sync(&store).await,
            Err(Error::Retired)
        ));
        assert!(matches!(
            Space::fetch(&store, replicas[0].space()).await,
            Err(Error::Retired)
        ));

        // A device bootstraps the new space from the folder and meets the publisher there.
        let fetched = Package::fetch(&store, &pin).await.unwrap();
        let joiner = Replica::open(
            dir.path().join("joiner.db"),
            Arc::new(Identity::join(package.destination(&pin).unwrap()).unwrap()),
        )
        .await
        .unwrap();
        joiner.install_checkpoint(&fetched, &pin).await.unwrap();
        assert_eq!(joiner.snapshot().await.unwrap().bookmarks, before);
        joiner.sync(&store).await.unwrap();
        target.sync(&store).await.unwrap();
        joiner.sync(&store).await.unwrap();
        assert_eq!(target.snapshot().await.unwrap().bookmarks, before);
        assert_eq!(joiner.snapshot().await.unwrap().pending_uploads, 0);

        // With a verified backup in hand, the old history leaves the folder.
        let backup = Package::verify_backup(&package.export().unwrap(), &pin).unwrap();
        let removed = replicas[0]
            .collect_history(&store, &package, &pin, &backup)
            .await
            .unwrap();
        assert_eq!(removed, 3);
        let old = vec![format!("day-sync-v5-{}", replicas[0].space())];
        let names = store.list(&old).await.unwrap();
        assert!(names.iter().all(|n| !n.starts_with("tx-")), "{names:?}");
        assert!(names.contains(&"retired.json".to_string()));
        for r in replicas.iter().chain([&target, &joiner]) {
            r.close().await.unwrap();
        }
    });
}
