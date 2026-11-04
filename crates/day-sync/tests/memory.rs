// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! The engine against the in-process store: the whole sync pass with no transport at all.
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use futures_lite::future::block_on;
use std::sync::Arc;

fn edit(id: &str, title: &str) -> Change {
    Change {
        id: id.into(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid".into(),
            title: title.into(),
        }),
    }
}

#[test]
fn memory_store_semantics() {
    block_on(async {
        let store = MemoryStore::new();
        let directory = vec!["space".to_string(), "nested".to_string()];
        assert!(matches!(
            store.list(&directory).await,
            Err(e) if e.kind == StoreErrorKind::NotFound
        ));
        store.ensure_directory(&directory).await.unwrap();
        assert_eq!(store.list(&directory).await.unwrap(), Vec::<String>::new());
        assert_eq!(
            store.list(&directory[..1]).await.unwrap(),
            vec!["nested".to_string()]
        );
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
        assert!(store.delete(&directory, "a").await.unwrap());
        assert!(!store.delete(&directory, "a").await.unwrap());
        assert_ne!(store.endpoint(), MemoryStore::new().endpoint());
    });
}

#[test]
fn two_replicas_converge_through_a_memory_store() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let store = MemoryStore::new();
        let owner = Arc::new(Identity::create_space().unwrap());
        let a = Replica::open(dir.path().join("a.db"), owner.clone())
            .await
            .unwrap();
        let b = Replica::open(
            dir.path().join("b.db"),
            Arc::new(Identity::join(owner.space.clone()).unwrap()),
        )
        .await
        .unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        a.commit(vec![edit(&id, "from a")]).await.unwrap();
        let report = a.sync(&store).await.unwrap();
        assert_eq!((report.uploaded, report.downloaded), (1, 0));

        // The second device finds the descriptor and the record.
        assert_eq!(
            Space::fetch(&store, owner.space()).await.unwrap(),
            owner.space
        );
        let report = b.sync(&store).await.unwrap();
        assert_eq!((report.downloaded, report.applied), (1, 1));
        assert_eq!(b.snapshot().await.unwrap().bookmarks[0].title, "from a");

        // Concurrent edits leave two candidates and one deterministic winner on both.
        a.commit(vec![edit(&id, "a again")]).await.unwrap();
        b.commit(vec![edit(&id, "b again")]).await.unwrap();
        a.sync(&store).await.unwrap();
        b.sync(&store).await.unwrap();
        a.sync(&store).await.unwrap();
        let (sa, sb) = (a.snapshot().await.unwrap(), b.snapshot().await.unwrap());
        assert_eq!(sa.bookmarks, sb.bookmarks);
        assert_eq!(sa.candidates, sb.candidates);
        assert_eq!(sa.candidates.len(), 2);

        let directory = vec![format!("day-sync-v5-{}", owner.space())];
        let objects = store.objects(&directory);
        assert_eq!(objects.keys().filter(|n| n.starts_with("tx-")).count(), 3);
        assert_eq!(
            objects.keys().filter(|n| n.starts_with("member-")).count(),
            2
        );
        assert!(objects.contains_key("space.json"));

        // A store that lost a published record is reported, never read as a deletion.
        let lost = objects
            .keys()
            .find(|n| n.starts_with("tx-"))
            .unwrap()
            .clone();
        assert!(store.lose(&directory, &lost));
        assert!(matches!(
            a.sync(&store).await,
            Err(Error::RemoteHistoryMissing)
        ));
        assert_eq!(a.snapshot().await.unwrap().bookmarks, sa.bookmarks);

        // A replica never mixes two stores.
        assert!(matches!(
            a.sync(&MemoryStore::new()).await,
            Err(Error::WrongEndpoint)
        ));
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
