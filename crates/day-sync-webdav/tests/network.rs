// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
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
use std::fmt::Write as _;
mod support;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use support::{Fixture, open};
#[derive(Clone, Copy, Default, Debug)]
enum Fault {
    #[default]
    None,
    LostAck,
    ContendedControl,
    PartialPut,
    CorruptPut,
    FalseSuccess,
    CorruptRead,
    TruncatedRead,
    Status(u16),
    IncompleteListing,
}
#[derive(Default)]
struct Store {
    files: BTreeMap<String, Vec<u8>>,
    fault: Fault,
    fault_controls: bool,
    puts: usize,
    collections: Vec<String>,
    mkdir_status: Option<u16>,
    delete_status: Option<u16>,
    lose_move_ack: bool,
    rival_at_move: Option<Vec<u8>>,
}
fn move_fixture_file(
    state: &mut Store,
    path: &str,
    headers: &[(String, String)],
) -> Result<Reply, http::HttpError> {
    let destination = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("Destination"))
        .unwrap()
        .1
        .clone();
    assert!(
        headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("Overwrite") && v == "F")
    );
    if let Some(other) = state.rival_at_move.take() {
        state.files.insert(destination.clone(), other);
    }
    if state.files.contains_key(&destination) {
        return Ok(Reply::new(412, vec![]));
    }
    let bytes = state.files.remove(path).unwrap();
    state.files.insert(destination, bytes);
    if std::mem::take(&mut state.lose_move_ack) {
        Err(http::HttpError::Connect)
    } else {
        Ok(Reply::new(201, vec![]))
    }
}
fn setup() -> (WebDavStore, Arc<Mutex<Store>>, Simulation) {
    let store = Arc::new(Mutex::new(Store::default()));
    let state = store.clone();
    let sim = Simulation::default();
    sim.handle(|_,_|true,move|request|{
        let mut s=state.lock().unwrap();let path=request.url.clone();
        match request.method.as_str(){
            "MKCOL"=>{let exists=s.collections.contains(&path);if !exists{s.collections.push(path);}Ok(Reply::new(if exists{s.mkdir_status.unwrap_or(405)}else{201},vec![]))},
            "PUT"=>{
                let bytes = match request.body {
                    PreparedBody::Bytes(b) => b.as_ref().clone(),
                    PreparedBody::File { path, .. } => std::fs::read(path).unwrap(),
                    _ => panic!("test expects a file or bytes"),
                };
                if !path.contains("/tx-") && !s.fault_controls{s.files.insert(path,bytes);return Ok(Reply::new(201,vec![]));}
                s.puts+=1;let fault=std::mem::take(&mut s.fault);
                match fault {
                    Fault::Status(code)=>Ok(Reply::new(code,vec![]).header("Retry-After","120")),
                    Fault::CorruptPut => {
                        s.files.insert(path, vec![0; bytes.len()]);
                        Ok(Reply::new(201, vec![]))
                    }
                    Fault::FalseSuccess=>Ok(Reply::new(201,vec![])),
                    Fault::PartialPut=>{s.files.insert(path,bytes[..bytes.len()/2].to_vec());Err(http::HttpError::Connect)},
                    Fault::LostAck=>{s.files.insert(path,bytes);Err(http::HttpError::Connect)},
                    Fault::ContendedControl=>{s.files.insert(path,bytes);Ok(Reply::new(403,vec![]))},
                    _=>{s.files.insert(path,bytes);s.fault=fault;Ok(Reply::new(201,vec![]))}
                }
            },
            "MOVE" => move_fixture_file(&mut s, &path, &request.headers),
            "DELETE"=>{if let Some(code)=s.delete_status.take(){Ok(Reply::new(code,vec![]).header("Retry-After","120"))}else{s.files.remove(&path);Ok(Reply::new(204,vec![]))}},
            "GET"=>{
                let Some(bytes)=s.files.get(&path).cloned() else{return Ok(Reply::new(404,vec![]));};
                match if path.contains("/tx-"){s.fault}else{Fault::None} {
                    Fault::CorruptRead=>{s.fault=Fault::None;Ok(Reply::new(200,b"corrupt fixture".to_vec()))},
                    Fault::TruncatedRead=>{s.fault=Fault::None;let mut r=Reply::new(200,bytes);r.chunks=vec![b"partial".to_vec()];r.fail_after_chunks=Some(1);Ok(r)},
                    _=>Ok(Reply::new(200,bytes))
                }
            },
            "PROPFIND"=>{
                let zero=request.headers.iter().any(|(k,v)|k.eq_ignore_ascii_case("depth")&&v=="0");
                let mut xml=String::from("<d:multistatus xmlns:d='DAV:'>");
                if !matches!(s.fault,Fault::IncompleteListing){let _=write!(xml,"<d:response><d:href>{path}</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");}
                if !zero{for name in s.files.keys().filter(|p|p.starts_with(&path)){let _=write!(xml,"<d:response><d:href>{name}</d:href><d:propstat><d:prop><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");}}
                xml+="</d:multistatus>";Ok(Reply::new(207,xml.into_bytes()))
            },_=>panic!("unexpected method")
        }
    });
    let session = http::Session::new();
    session.set_simulation(Some(sim.clone()));
    let mut policy = Policy::default();
    policy.retry.max_attempts = 1;
    (
        WebDavStore::new(
            Client::builder(Endpoint::new("https://fixture.invalid/dav/").unwrap())
                .session(session)
                .policy(policy)
                .build(),
        ),
        store,
        sim,
    )
}
fn edit() -> Change {
    Change {
        id: uuid::Uuid::new_v4().to_string(),
        value: Some(BookmarkValue {
            url: "https://fixture.invalid".into(),
            title: "Synthetic offline bookmark".into(),
        }),
    }
}
#[test]
fn uncertain_writes_reconcile_exact_bytes_without_duplicate_transactions() {
    block_on(async {
        for fault in [
            Fault::LostAck,
            Fault::PartialPut,
            Fault::FalseSuccess,
            Fault::CorruptRead,
            Fault::TruncatedRead,
            Fault::Status(401),
            Fault::Status(403),
            Fault::Status(429),
            Fault::Status(503),
            Fault::Status(507),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let space = Fixture::new().await;
            let path = dir.path().join("a.db");
            let (client, state, _) = setup();
            let a = open(&path, &space).await.unwrap();
            a.commit(vec![edit()]).await.unwrap();
            state.lock().unwrap().fault = fault;
            assert!(a.sync(&client).await.is_err(), "{fault:?}");
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1, "{fault:?}");
            a.close().await.unwrap();
            let a = open(&path, &space).await.unwrap();
            a.sync(&client).await.unwrap();
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
            let b = open(dir.path().join("b.db"), &space).await.unwrap();
            b.sync(&client).await.unwrap();
            assert_eq!(
                a.snapshot().await.unwrap().bookmarks,
                b.snapshot().await.unwrap().bookmarks
            );
            assert_eq!(b.snapshot().await.unwrap().transactions, 1);
            assert_eq!(
                state
                    .lock()
                    .unwrap()
                    .files
                    .keys()
                    .filter(|p| p.contains("/tx-"))
                    .count(),
                1
            );
            if matches!(
                fault,
                Fault::LostAck | Fault::CorruptRead | Fault::TruncatedRead
            ) {
                assert_eq!(state.lock().unwrap().puts, 1, "{fault:?}");
            }
            a.close().await.unwrap();
            b.close().await.unwrap();
        }
    });
}
#[test]
fn network_admission_preserves_outbox_and_incomplete_listing_is_not_success() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let a = open(d.path().join("a.db"), &Fixture::new().await)
            .await
            .unwrap();
        let (client, state, sim) = setup();
        a.commit(vec![edit()]).await.unwrap();
        for state in [
            NetworkState::Offline,
            NetworkState::Constrained,
            NetworkState::Expensive,
        ] {
            client.client().set_network_state(state);
            assert!(a.sync(&client).await.is_err());
            assert!(sim.requests().is_empty());
            assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1);
        }
        client.client().set_network_state(NetworkState::Available);
        a.sync(&client).await.unwrap();
        state.lock().unwrap().fault = Fault::IncompleteListing;
        std::assert_matches!(
            a.sync(&client).await,
            Err(day_sync::Error::Store(e)) if e.kind == StoreErrorKind::IncompleteListing);
        assert_eq!(a.snapshot().await.unwrap().bookmarks.len(), 1);
        a.close().await.unwrap();
    });
}
#[test]
fn transport_endpoint_cannot_change_after_receipts_are_bound() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let a = open(d.path().join("a.db"), &Fixture::new().await)
            .await
            .unwrap();
        let (client, _, _) = setup();
        a.sync(&client).await.unwrap();
        let other = WebDavStore::new(
            Client::builder(Endpoint::new("https://other.invalid/dav/").unwrap()).build(),
        );
        std::assert_matches!(a.sync(&other).await, Err(day_sync::Error::WrongEndpoint));
        a.close().await.unwrap();
    });
}

#[test]
fn cancelling_in_flight_sync_releases_admission_without_losing_intent() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
        time::Duration,
    };
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let a = open(d.path().join("a.db"), &Fixture::new().await)
            .await
            .unwrap();
        let (client, _, sim) = setup();
        a.commit(vec![edit()]).await.unwrap();
        sim.set_conditions(http::simulation::Conditions {
            latency: Duration::from_secs(30),
            ..Default::default()
        });
        let mut future = Box::pin(a.sync(&client));
        for _ in 0..1000 {
            std::assert_matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            );
            if !sim.requests().is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(!sim.requests().is_empty());
        std::assert_matches!(a.sync(&client).await, Err(day_sync::Error::Busy));
        drop(future);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1);
        sim.set_conditions(http::simulation::Conditions::default());
        a.sync(&client).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        a.close().await.unwrap();
    });
}

#[test]
fn unchanged_sync_uses_listing_without_redownloading_membership_or_transactions() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let f = Fixture::new().await;
        let a = open(d.path().join("a"), &f).await.unwrap();
        let (client, _, sim) = setup();
        a.commit(vec![edit()]).await.unwrap();
        a.sync(&client).await.unwrap();
        let before = sim.requests().len();
        assert_eq!(a.sync(&client).await.unwrap(), SyncReport::default());
        let requests = sim.requests();
        assert!(!requests[before..].iter().any(|r| r.method == "PUT"
            || (r.method == "GET"
                && !r.url.ends_with("/space.json")
                && !r.url.ends_with("/retired.json"))));
        a.close().await.unwrap();
    });
}

#[test]
fn lost_membership_upload_acknowledgement_reconciles_before_publishing_data() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let f = Fixture::new().await;
        let a = open(d.path().join("a"), &f).await.unwrap();
        let (client, state, sim) = setup();
        a.commit(vec![edit()]).await.unwrap();
        {
            let mut s = state.lock().unwrap();
            s.fault_controls = true;
            s.fault = Fault::LostAck;
        }
        a.sync(&client).await.unwrap();
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        let first = state.lock().unwrap().files.keys().next().unwrap().clone();
        assert!(first.contains("/member-"));
        a.sync(&client).await.unwrap();
        assert_eq!(
            sim.requests()
                .iter()
                .filter(|r| r.method == "PUT" && r.url == first)
                .count(),
            1
        );
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 0);
        a.close().await.unwrap();
    });
}

#[test]
fn real_server_contention_responses_require_verified_remote_state() {
    block_on(async {
        for code in [403, 405, 409, 423] {
            let d = tempfile::tempdir().unwrap();
            let a = open(d.path().join("a"), &Fixture::new().await)
                .await
                .unwrap();
            let (client, state, _) = setup();
            {
                let mut s = state.lock().unwrap();
                s.fault_controls = true;
                s.fault = Fault::ContendedControl;
            }
            a.sync(&client).await.unwrap();
            state.lock().unwrap().mkdir_status = Some(code);
            a.sync(&client).await.unwrap();
            a.close().await.unwrap();
        }
    });
}
#[test]
fn server_history_loss_is_reported_without_discarding_local_state() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let a = open(d.path().join("a"), &Fixture::new().await)
            .await
            .unwrap();
        let (client, state, _) = setup();
        a.commit(vec![edit()]).await.unwrap();
        a.sync(&client).await.unwrap();
        let before = a.snapshot().await.unwrap();
        let lost = {
            let mut s = state.lock().unwrap();
            let path = s.files.keys().find(|p| p.contains("/tx-")).unwrap().clone();
            let bytes = s.files.remove(&path).unwrap();
            (path, bytes)
        };
        std::assert_matches!(
            a.sync(&client).await,
            Err(day_sync::Error::RemoteHistoryMissing)
        );
        let after = a.snapshot().await.unwrap();
        assert_eq!(after.candidates, before.candidates);
        assert_eq!(after.frontier, before.frontier);
        assert_eq!(after.transactions, 0); // repaired snapshot now durably publishes the prefix
        a.sync(&client).await.unwrap(); // recovery no longer depends on manual restoration

        state.lock().unwrap().files.insert(lost.0, lost.1);
        a.sync(&client).await.unwrap();
        a.close().await.unwrap();
    });
}

#[test]
fn persistent_bad_data_does_not_block_independent_records_or_advance_missing_dependencies() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let f = Fixture::new().await;
        let a = open(d.path().join("a"), &f).await.unwrap();
        let b = open(d.path().join("b"), &f).await.unwrap();
        let c = open(d.path().join("c"), &f).await.unwrap();
        let (client, state, _) = setup();
        support::deliver_members(&f.admin, &a).await;
        a.commit(vec![edit()]).await.unwrap();
        a.commit(vec![edit()]).await.unwrap();
        a.sync(&client).await.unwrap();
        let damaged = a.local_objects().await.unwrap().remove(0);
        {
            let mut store = state.lock().unwrap();
            let key = store
                .files
                .keys()
                .find(|k| k.ends_with(&damaged.filename))
                .unwrap()
                .clone();
            store
                .files
                .insert(key, b"synthetic permanently truncated object".to_vec());
        }
        b.commit(vec![edit()]).await.unwrap();
        std::assert_matches!(
            b.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { count: 1 })
        );
        std::assert_matches!(
            c.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { count: 1 })
        );
        let snapshot = c.snapshot().await.unwrap();
        assert_eq!(snapshot.bookmarks.len(), 1); // The independent actor's record applied.
        assert_eq!(snapshot.waiting, 1); // The corrupt predecessor's successor did not.
        assert!(!snapshot.frontier.contains_key(a.actor()));
        assert_eq!(snapshot.frontier.get(b.actor()), Some(&1));
        assert!(!snapshot.poisoned);
        std::assert_matches!(
            c.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { .. })
        );
        {
            let mut store = state.lock().unwrap();
            let key = store
                .files
                .keys()
                .find(|k| k.ends_with(&damaged.filename))
                .unwrap()
                .clone();
            store.files.insert(key, damaged.bytes);
        }
        c.sync(&client).await.unwrap();
        assert_eq!(c.snapshot().await.unwrap().bookmarks.len(), 3);
        assert_eq!(c.snapshot().await.unwrap().waiting, 0);
        for r in [a, b, c] {
            r.close().await.unwrap();
        }
    });
}

#[test]
fn partial_member_upload_is_repaired_before_data_is_published() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let f = Fixture::new().await;
        let a = open(d.path().join("a"), &f).await.unwrap();
        let b = open(d.path().join("b"), &f).await.unwrap();
        let (client, state, _) = setup();
        a.sync(&client).await.unwrap();
        b.commit(vec![edit()]).await.unwrap();
        {
            let mut s = state.lock().unwrap();
            s.fault_controls = true;
            s.fault = Fault::PartialPut;
        }
        assert!(b.sync(&client).await.is_err());
        assert_eq!(b.snapshot().await.unwrap().pending_uploads, 1);
        b.sync(&client).await.unwrap();
        a.sync(&client).await.unwrap();
        assert_eq!(
            a.snapshot().await.unwrap().bookmarks,
            b.snapshot().await.unwrap().bookmarks
        );
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}

#[test]
fn snapshot_deletion_respects_webdav_contention_rate_limits_and_quota_errors() {
    block_on(async {
        for status in [423, 429, 507] {
            let d = tempfile::tempdir().unwrap();
            let fixture = Fixture::new().await;
            let (client, state, _) = setup();
            let shared = state.clone();
            let armed = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let flag = armed.clone();
            let a = support::open_with_probe(
                d.path().join("a"),
                &fixture,
                Arc::new(move |point| {
                    if point == Checkpoint::SnapshotVerified
                        && flag.swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        shared.lock().unwrap().delete_status = Some(status);
                    }
                    Ok(())
                }),
            )
            .await
            .unwrap();
            a.commit(vec![edit()]).await.unwrap();
            a.sync(&client).await.unwrap();
            let error = a.compact(&client).await.unwrap_err();
            std::assert_matches!(error,day_sync::Error::Store(ref e) if e.retry_after==Some(std::time::Duration::from_secs(120)),
                "{error:?}");
            assert!(
                state
                    .lock()
                    .unwrap()
                    .files
                    .keys()
                    .any(|n| n.contains("/tx-"))
            );
            a.compact(&client).await.unwrap();
            let b = open(d.path().join("b"), &fixture).await.unwrap();
            b.sync(&client).await.unwrap();
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
fn response_bound_is_a_capacity_error_not_a_transient_outage() {
    let simulation = Simulation::new();
    simulation.handle(
        |_, _| true,
        |_| Ok(Reply::new(200, b"synthetic payload".to_vec())),
    );
    let session = http::Session::new();
    session.set_simulation(Some(simulation.clone()));
    let client = WebDavStore::new(
        Client::builder(Endpoint::new("https://fixture.invalid/dav/").unwrap())
            .session(session)
            .policy(Policy {
                max_response_bytes: 4,
                ..Policy::default()
            })
            .build(),
    );
    let error = block_on(day_sync::Store::get(&client, &[], "synthetic.bin")).unwrap_err();
    assert_eq!(error.kind, StoreErrorKind::Limit);
    assert!(std::error::Error::source(&error).is_some());
    assert_eq!(simulation.requests().len(), 1);
}

#[test]
fn named_publication_reconciles_failures_and_skips_existing_uploads() {
    use day_sync::Store as _;
    use std::io::Write as _;
    struct Full;
    impl std::io::Write for Full {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::ErrorKind::StorageFull.into())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    block_on(async {
        let (store, state, sim) = setup();
        let store = store.with_private_directory(".sync", "books").unwrap();
        let file = assets::AssetFile {
            directory: "books".into(),
            name: "Édition #1 + 100%.epub".into(),
        };
        let directory = [file.directory.clone()];
        let bytes = b"synthetic publication";
        let digest = "7e868dd739f0c090d5973a555fa6a47d8c427bd4ae4983fcd4e8d1e9cc5ae4a9";
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(bytes).unwrap();
        state.lock().unwrap().fault_controls = true;
        for fault in [Fault::PartialPut, Fault::CorruptPut] {
            state.lock().unwrap().fault = fault;
            assert!(
                assets::upload_file(&store, file.clone(), source.path(), digest, |_| {})
                    .await
                    .is_err()
            );
            assert!(state.lock().unwrap().files.is_empty());
        }
        // The server completes a staging PUT and the final MOVE but loses both replies.
        {
            let mut remote = state.lock().unwrap();
            remote.fault = Fault::LostAck;
            remote.lose_move_ack = true;
        }
        let mut sent = Vec::new();
        assets::upload_file(&store, file.clone(), source.path(), digest, |n| {
            sent.push(n);
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sent.first(), Some(&0));
        assert_eq!(sent.last(), Some(&(bytes.len() as u64)));
        assert!(sent.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(
            store.list(&directory).await.unwrap(),
            std::slice::from_ref(&file.name)
        );
        let requests = sim.requests();
        assert!(
            requests
                .iter()
                .filter(|r| r.method == "PUT")
                .all(|r| r.url.contains("/.sync/day-transfers-v1/"))
        );
        let before = requests.len();
        assets::upload_file(&store, file.clone(), source.path(), digest, |_| {})
            .await
            .unwrap()
            .unwrap();
        let mut other = tempfile::NamedTempFile::new().unwrap();
        other.write_all(b"x").unwrap();
        let hash = "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881";
        assert!(
            assets::upload_file(&store, file.clone(), other.path(), hash, |_| {})
                .await
                .unwrap()
                .is_none()
        );
        let requests = sim.requests();
        assert!(
            requests[before..]
                .iter()
                .all(|r| r.method == "GET" && r.url.contains("/books/"))
        );
        assert_eq!(
            store.get(&directory, &file.name).await.unwrap().unwrap(),
            bytes
        );
        state.lock().unwrap().rival_at_move = Some(b"concurrent edition".to_vec());
        let rival = assets::AssetFile {
            directory: file.directory.clone(),
            name: "Concurrent.epub".into(),
        };
        assert!(
            assets::upload_file(&store, rival.clone(), source.path(), digest, |_| {})
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.get(&directory, &rival.name).await.unwrap().unwrap(),
            b"concurrent edition"
        );
        assert!(
            state
                .lock()
                .unwrap()
                .files
                .keys()
                .all(|p| !p.contains("/day-transfers-v1/"))
        );
        let private = ["records".to_owned()];
        store.ensure_directory(&private).await.unwrap();
        store.put(&private, "one", vec![1]).await.unwrap();
        assert_eq!(
            store.create(&private, "two", vec![2]).await.unwrap(),
            Created::Created
        );
        assert_eq!(store.get(&private, "one").await.unwrap(), Some(vec![1]));
        assert!(matches!(
            store.read_object(&private, "two", None).await.unwrap(),
            day_sync::store::ObjectRead::Bytes(_, _)
        ));
        let mut names = store.list(&private).await.unwrap();
        names.sort();
        assert_eq!(names, ["one", "two"]);
        assert!(store.delete(&private, "one").await.unwrap());
        assert!(
            sim.requests()
                .iter()
                .filter(|r| r.url.contains("/records/"))
                .all(|r| r.url.contains("/.sync/records/"))
        );
        assert_eq!(
            store
                .read_file(&directory, &file.name, &mut Full, 1024)
                .await
                .unwrap_err()
                .kind,
            StoreErrorKind::InsufficientStorage
        );
    });
}
