// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
use day_sync::*;
use day_sync_webdav::{WebDavStore, webdav::*};
use futures_lite::future::block_on;
use std::sync::Arc;
mod support;
use support::{Fixture, open};
fn edit() -> Change {
    Change {
        id: uuid::Uuid::new_v4().to_string(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid/crash".into(),
            title: "Synthetic crash test".into(),
        }),
    }
}
fn client(url: &str) -> WebDavStore {
    WebDavStore::new(Client::builder(Endpoint::loopback(url).unwrap()).build())
}
#[test]
#[ignore = "subprocess entry point; only the parent supplies its synthetic fixture"]
fn crash_child() {
    block_on(async {
        let point = std::env::var("DAY_SYNC_CRASH_POINT").unwrap();
        let probe = Arc::new(move |p: Checkpoint| {
            if format!("{p:?}") == point {
                std::process::exit(86);
            }
            Ok(())
        });
        let a = Replica::open_with_probe(
            std::env::var("DAY_SYNC_DB").unwrap(),
            Arc::new(
                Identity::from_bytes(
                    &std::fs::read(std::env::var("DAY_SYNC_IDENTITY").unwrap()).unwrap(),
                )
                .unwrap(),
            ),
            probe,
        )
        .await
        .unwrap();
        if std::env::var("DAY_SYNC_INCOMING").unwrap() == "false" {
            a.commit(vec![edit()]).await.unwrap();
        }
        a.sync(&client(&std::env::var("DAY_SYNC_URL").unwrap()))
            .await
            .unwrap();
        panic!("checkpoint not reached")
    });
}
#[test]
fn eleven_process_termination_boundaries_recover_without_partial_transactions() {
    block_on(async {
        let server = testing::Server::start().unwrap();
        let client = client(&server.url());
        for point in [
            Checkpoint::BeforeJournal,
            Checkpoint::AfterJournal,
            Checkpoint::LocalCommitted,
            Checkpoint::BeforeUpload,
            Checkpoint::Uploaded,
            Checkpoint::Verified,
            Checkpoint::BeforePublishReceipt,
            Checkpoint::Published,
            Checkpoint::InboxStored,
            Checkpoint::BeforeApplyCommit,
            Checkpoint::Applied,
        ] {
            let d = tempfile::tempdir().unwrap();
            let path = d.path().join("crashed.db");
            let space = Fixture::new().await;
            let incoming = matches!(
                point,
                Checkpoint::InboxStored | Checkpoint::BeforeApplyCommit | Checkpoint::Applied
            );
            if incoming {
                let source = open(d.path().join("source.db"), &space).await.unwrap();
                source.commit(vec![edit()]).await.unwrap();
                source.sync(&client).await.unwrap();
                source.close().await.unwrap();
            }
            // Public test identity, saved alongside the database.
            let keys = d.path().join("fixture-keys.json");
            std::fs::write(&keys, space.credentials(&path).await.to_bytes().unwrap()).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "crash_child", "--ignored", "--nocapture"])
                .env("DAY_SYNC_CRASH_POINT", format!("{point:?}"))
                .env("DAY_SYNC_DB", &path)
                .env("DAY_SYNC_IDENTITY", &keys)
                .env("DAY_SYNC_URL", server.url())
                .env("DAY_SYNC_INCOMING", incoming.to_string())
                .output()
                .unwrap();
            assert_eq!(
                status.status.code(),
                Some(86),
                "{point:?}: {}",
                String::from_utf8_lossy(&status.stderr)
            );
            let a = open(&path, &space).await.unwrap();
            let before = a.snapshot().await.unwrap();
            let rolled_back = matches!(point, Checkpoint::BeforeJournal | Checkpoint::AfterJournal);
            assert_eq!(
                before.transactions,
                if rolled_back { 0 } else { 1 },
                "{point:?}"
            );
            if matches!(
                point,
                Checkpoint::InboxStored | Checkpoint::BeforeApplyCommit
            ) {
                assert_eq!(before.waiting, 1);
                assert!(before.bookmarks.is_empty());
                assert!(before.frontier.is_empty());
            }
            a.sync(&client).await.unwrap();
            let after = a.snapshot().await.unwrap();
            assert_eq!(
                after.bookmarks.len(),
                if rolled_back { 0 } else { 1 },
                "{point:?}"
            );
            assert_eq!(after.pending_uploads, 0);
            assert_eq!(after.waiting, 0);
            if incoming {
                assert!(a.local_objects().await.unwrap().is_empty());
            }
            a.close().await.unwrap();
            let db = rusqlite::Connection::open(&path).unwrap();
            assert_eq!(
                db.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                    .unwrap(),
                "ok"
            );
            assert!(
                !db.prepare("PRAGMA foreign_key_check")
                    .unwrap()
                    .exists([])
                    .unwrap()
            );
            assert_eq!(
                db.query_row("SELECT mode FROM _day_sync_guard", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
    });
}
