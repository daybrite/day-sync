// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Compaction over WebDAV: every request of a compacting pass can fail, before or after the
//! server acts on it. Also exercise repair after replacement corruption and process reopening.
#![cfg(not(target_arch = "wasm32"))]
use day_sync::*;
use day_sync_webdav::{
    WebDavStore,
    webdav::{
        http::{
            self,
            simulation::{Reply, Simulation},
            transport::PreparedBody,
        },
        *,
    },
};
use futures_lite::future::block_on;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
#[derive(Clone, Default)]
struct Server {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
    requests: Vec<(String, String)>,
    /// Fail request number `n`: before the server acts (false) or after (true).
    fail: Option<(usize, bool)>,
    /// Answer request number `n` with this status and a Retry-After.
    status: Option<(usize, u16)>,
}
fn server() -> (WebDavStore, Arc<Mutex<Server>>) {
    let state = Arc::new(Mutex::new(Server::default()));
    let st = state.clone();
    let simulation = Simulation::new();
    simulation.handle(|_, _| true, move |r| {
        let mut s = st.lock().unwrap();
        let n = s.requests.len();
        s.requests.push((r.method.clone(), r.url.clone()));
        let fail = s.fail.filter(|(at, _)| *at == n);
        if fail.is_some() {
            s.fail = None;
        }
        if fail == Some((n, false)) {
            return Err(http::HttpError::Connect);
        }
        if let Some((at, code)) = s.status.filter(|(at, _)| *at == n) {
            let _ = at;
            s.status = None;
            return Ok(Reply::new(code, vec![]).header("Retry-After", "1"));
        }
        let none_match = r
            .headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("if-none-match") && v == "*");
        let reply = match r.method.as_str() {
            "MKCOL" => Reply::new(if s.dirs.insert(r.url.clone()) { 201 } else { 405 }, vec![]),
            "PUT" => {
                let PreparedBody::Bytes(b) = r.body else {
                    panic!("bounded body")
                };
                if none_match && s.files.contains_key(&r.url) {
                    Reply::new(412, vec![])
                } else {
                    s.files.insert(r.url.clone(), b.to_vec());
                    Reply::new(201, vec![])
                }
            }
            "DELETE" => Reply::new(
                if s.files.remove(&r.url).is_some() { 204 } else { 404 },
                vec![],
            ),
            "GET" => match s.files.get(&r.url).cloned() {
                Some(bytes) => Reply::new(200, bytes),
                None => Reply::new(404, vec![]),
            },
            "PROPFIND" => {
                let mut xml = format!(
                    "<d:multistatus xmlns:d='DAV:'><d:response><d:href>{}</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>",
                    r.url
                );
                if !r
                    .headers
                    .iter()
                    .any(|(k, v)| k.eq_ignore_ascii_case("depth") && v == "0")
                {
                    for name in s.files.keys().filter(|p| p.starts_with(&r.url)) {
                        xml += &format!(
                            "<d:response><d:href>{name}</d:href><d:propstat><d:prop><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>"
                        );
                    }
                }
                xml += "</d:multistatus>";
                Reply::new(207, xml.into_bytes())
            }
            _ => panic!("unexpected request"),
        };
        if fail == Some((n, true)) {
            return Err(http::HttpError::Connect);
        }
        Ok(reply)
    });
    let session = http::Session::new();
    session.set_simulation(Some(simulation));
    let mut policy = Policy::default();
    policy.retry.max_attempts = 1;
    (
        WebDavStore::new(
            Client::builder(Endpoint::new("https://fixture.invalid/dav/").unwrap())
                .session(session)
                .policy(policy)
                .build(),
        ),
        state,
    )
}
fn change(id: usize, title: &str) -> Change {
    Change {
        id: format!("00000000-0000-4000-8000-{id:012}"),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid".into(),
            title: title.into(),
        }),
    }
}

async fn prepare(
    path: &std::path::Path,
    owner: Arc<Identity>,
    store: &WebDavStore,
    state: &Arc<Mutex<Server>>,
    repair: bool,
) -> Replica {
    let a = Replica::open(path, owner).await.unwrap();
    for i in 0..20 {
        a.commit(vec![change(i, "synthetic history")])
            .await
            .unwrap();
    }
    a.sync(store).await.unwrap();
    if repair {
        let r = a
            .compact_with_options(
                store,
                SyncOptions {
                    max_deletions: 0,
                    ..SyncOptions::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.pending_deletions, 20);
        let mut s = state.lock().unwrap();
        let part = s
            .files
            .keys()
            .find(|n| n.contains(".part-"))
            .unwrap()
            .clone();
        s.files
            .insert(part, b"synthetic corrupt replacement".to_vec());
    }
    state.lock().unwrap().requests.clear();
    a
}

#[test]
fn every_request_can_fail_before_or_after_server_effect_then_reopen_and_converge() {
    block_on(async {
        for repair in [false, true] {
            let d = tempfile::tempdir().unwrap();
            let owner = Arc::new(Identity::create_space().unwrap());
            let (store, state) = server();
            let a = prepare(&d.path().join("a"), owner.clone(), &store, &state, repair).await;
            let options = SyncOptions {
                max_deletions: 7,
                ..SyncOptions::default()
            };
            let r = a.compact_with_options(&store, options).await.unwrap();
            assert_eq!(r.pending_deletions, 13);
            assert_eq!(r.deletion_requests, 7);
            let count = state.lock().unwrap().requests.len();
            a.close().await.unwrap();
            for after in [false, true] {
                for at in 0..count {
                    let d = tempfile::tempdir().unwrap();
                    let (store, state) = server();
                    let a =
                        prepare(&d.path().join("a"), owner.clone(), &store, &state, repair).await;
                    state.lock().unwrap().fail = Some((at, after));
                    let _ = a.compact_with_options(&store, options).await;
                    assert!(
                        state.lock().unwrap().fail.is_none(),
                        "fault not reached repair={repair} at={at} after={after}"
                    );
                    a.close().await.unwrap();
                    drop(a);
                    let a = Replica::open(d.path().join("a"), owner.clone())
                        .await
                        .unwrap();
                    a.commit(vec![change(20, "after restart")]).await.unwrap();
                    a.sync(&store).await.unwrap();
                    a.compact(&store).await.unwrap();
                    a.sync(&store).await.unwrap();
                    let b = Replica::open(
                        d.path().join("b"),
                        Arc::new(Identity::join(owner.space.clone()).unwrap()),
                    )
                    .await
                    .unwrap();
                    b.sync(&store).await.unwrap();
                    assert_eq!(
                        a.snapshot().await.unwrap().candidates,
                        b.snapshot().await.unwrap().candidates,
                        "repair={repair} at={at} after={after}"
                    );
                    assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 21);
                    assert!(
                        !state
                            .lock()
                            .unwrap()
                            .files
                            .keys()
                            .any(|n| n.contains("/tx-"))
                    );
                    a.close().await.unwrap();
                    b.close().await.unwrap();
                }
            }
        }
    });
}

#[test]
fn write_refusals_keep_inputs_and_retry_after_metadata() {
    block_on(async {
        for code in [423, 429, 507, 503] {
            let owner = Arc::new(Identity::create_space().unwrap());
            let d = tempfile::tempdir().unwrap();
            let (probe, probe_state) = server();
            let b = prepare(
                &d.path().join("probe"),
                owner.clone(),
                &probe,
                &probe_state,
                false,
            )
            .await;
            b.compact(&probe).await.unwrap();
            let first_put = probe_state
                .lock()
                .unwrap()
                .requests
                .iter()
                .position(|(m, n)| m == "PUT" && n.contains("/snap-"))
                .unwrap();
            b.close().await.unwrap();
            let (store, state) = server();
            let a = prepare(&d.path().join("a"), owner, &store, &state, false).await;
            state.lock().unwrap().status = Some((first_put, code));
            let e = a.compact(&store).await.unwrap_err();
            let day_sync::Error::Store(e) = e else {
                panic!("{code}: {e:?}")
            };
            assert!(e.retry_after.is_some(), "{code}: {e:?}");
            assert_eq!(
                state
                    .lock()
                    .unwrap()
                    .files
                    .keys()
                    .filter(|n| n.contains("/tx-"))
                    .count(),
                20
            );
            assert_eq!(a.snapshot().await.unwrap().transactions, 20);
            a.compact(&store).await.unwrap();
            a.close().await.unwrap();
        }
    });
}

async fn prepare_outbox(
    path: &std::path::Path,
    owner: Arc<Identity>,
    store: &WebDavStore,
    state: &Arc<Mutex<Server>>,
    residue: u8,
) -> Replica {
    let a = Replica::open(path, owner).await.unwrap();
    a.sync(store).await.unwrap();
    for i in 0..65 {
        a.commit(vec![change(i, "offline")]).await.unwrap();
    }
    if residue != 0 {
        let object = a.local_objects().await.unwrap().remove(0);
        let bytes = if residue == 1 {
            object.bytes
        } else {
            vec![b'x']
        };
        store
            .put(
                &[format!("day-sync-v5-{}", a.space())],
                &object.filename,
                bytes,
            )
            .await
            .unwrap();
    }
    state.lock().unwrap().requests.clear();
    a
}

#[test]
fn early_outbox_publication_survives_every_webdav_request_and_partial_residue() {
    block_on(async {
        for residue in 0..3 {
            let d = tempfile::tempdir().unwrap();
            let owner = Arc::new(Identity::create_space().unwrap());
            let (store, state) = server();
            let a = prepare_outbox(
                &d.path().join("baseline"),
                owner.clone(),
                &store,
                &state,
                residue,
            )
            .await;
            let report = a.sync(&store).await.unwrap();
            assert_eq!(report.uploaded, usize::from(residue != 0));
            assert_eq!(report.snapshots_published, 1);
            let count = state.lock().unwrap().requests.len();
            a.close().await.unwrap();
            for after in [false, true] {
                for at in 0..count {
                    let d = tempfile::tempdir().unwrap();
                    let (store, state) = server();
                    let a =
                        prepare_outbox(&d.path().join("a"), owner.clone(), &store, &state, residue)
                            .await;
                    state.lock().unwrap().fail = Some((at, after));
                    let _ = a.sync(&store).await;
                    assert!(
                        state.lock().unwrap().fail.is_none(),
                        "unreached residue={residue} at={at} after={after}"
                    );
                    a.close().await.unwrap();
                    drop(a);
                    let a = Replica::open(d.path().join("a"), owner.clone())
                        .await
                        .unwrap();
                    a.commit(vec![change(65, "after restart")]).await.unwrap();
                    a.sync(&store).await.unwrap();
                    a.compact(&store).await.unwrap();
                    let b = Replica::open(
                        d.path().join("b"),
                        Arc::new(Identity::join(owner.space.clone()).unwrap()),
                    )
                    .await
                    .unwrap();
                    b.sync(&store).await.unwrap();
                    assert_eq!(b.snapshot().await.unwrap().bookmarks.len(), 66);
                    assert_eq!(
                        a.snapshot().await.unwrap().candidates,
                        b.snapshot().await.unwrap().candidates,
                        "residue={residue} at={at} after={after}"
                    );
                    assert!(
                        !state
                            .lock()
                            .unwrap()
                            .files
                            .keys()
                            .any(|n| n.contains("/tx-"))
                    );
                    a.close().await.unwrap();
                    b.close().await.unwrap();
                }
            }
        }
    });
}
