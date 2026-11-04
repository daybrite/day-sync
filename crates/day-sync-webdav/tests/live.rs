// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use day_sync_webdav::{WebDavStore, webdav::*};
use futures_lite::future::block_on;
mod support;
use support::{Fixture, open};
fn edit(id: &str, title: &str) -> Change {
    Change {
        id: id.into(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid/live".into(),
            title: title.into(),
        }),
    }
}
fn lifecycle(url: &str, provider: http::Provider, auth: bool) {
    block_on(async {
        let session = http::Session::new();
        session.set_provider(provider);
        let mut builder = Client::builder(Endpoint::loopback(url).unwrap()).session(session);
        if auth {
            builder = builder.authentication(Authentication::Basic {
                username: std::env::var("DAV_FIXTURE_USER")
                    .unwrap_or_else(|_| "day_fixture".into()),
                password: std::env::var("DAV_FIXTURE_PASSWORD").unwrap(),
            });
        }
        let client = WebDavStore::new(builder.build());
        let dir = tempfile::tempdir().unwrap();
        let space = Fixture::new().await;
        let object = uuid::Uuid::new_v4().to_string();
        let a = open(dir.path().join("a.db"), &space).await.unwrap();
        let b = open(dir.path().join("b.db"), &space).await.unwrap();
        a.commit(vec![edit(&object, "initial")]).await.unwrap();
        if auth && std::env::var_os("DAY_SYNC_FAULT_PROXY").is_some() {
            assert!(
                a.sync(&client).await.is_err(),
                "proxy must lose the first PUT acknowledgement"
            );
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1);
        }
        a.sync(&client).await.unwrap();
        b.sync(&client).await.unwrap();
        a.commit(vec![edit(&object, "offline A")]).await.unwrap();
        b.commit(vec![edit(&object, "offline B")]).await.unwrap();
        a.sync(&client).await.unwrap();
        b.sync(&client).await.unwrap();
        a.sync(&client).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().candidates,
            b.snapshot().await.unwrap().candidates
        );
        assert_eq!(a.snapshot().await.unwrap().candidates.len(), 2);
        a.commit(vec![Change {
            id: object.clone(),
            value: None,
        }])
        .await
        .unwrap();
        a.sync(&client).await.unwrap();
        b.sync(&client).await.unwrap();
        assert!(b.snapshot().await.unwrap().bookmarks.is_empty());
        b.commit(vec![edit(&object, "explicit resurrection")])
            .await
            .unwrap();
        b.sync(&client).await.unwrap();
        a.sync(&client).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().bookmarks,
            b.snapshot().await.unwrap().bookmarks
        );
        assert_eq!(a.snapshot().await.unwrap().transactions, 5);
        a.close().await.unwrap();
        b.close().await.unwrap();
        let c = open(dir.path().join("fresh.db"), &space).await.unwrap();
        let report = c.sync(&client).await.unwrap();
        assert_eq!(report.downloaded, 5);
        assert_eq!(report.applied, 5);
        assert_eq!(
            c.snapshot().await.unwrap().bookmarks[0].title,
            "explicit resurrection"
        );
        assert_eq!(c.sync(&client).await.unwrap(), SyncReport::default());
        c.close().await.unwrap();
        // The UUID namespace was freshly created by this test; never delete the supplied root.
        client
            .client()
            .delete(
                &ResourcePath::from_segments([format!("day-sync-v5-{space}")], true).unwrap(),
                Condition::Unconditional,
            )
            .await
            .unwrap();
    });
}
#[test]
fn native_loopback() {
    let server = testing::Server::start().unwrap();
    lifecycle(&server.url(), http::Provider::Native, false);
}
#[cfg(feature = "reqwest")]
#[test]
fn reqwest_loopback() {
    let server = testing::Server::start().unwrap();
    lifecycle(&server.url(), http::Provider::Reqwest, false);
}
#[test]
#[ignore = "isolated Apache/Nextcloud fixture only"]
fn native_external() {
    lifecycle(
        &std::env::var("DAV_FIXTURE_URL").unwrap(),
        http::Provider::Native,
        true,
    );
}
#[cfg(feature = "reqwest")]
#[test]
#[ignore = "isolated Apache/Nextcloud fixture only"]
fn reqwest_external() {
    lifecycle(
        &std::env::var("DAV_FIXTURE_URL").unwrap(),
        http::Provider::Reqwest,
        true,
    );
}
