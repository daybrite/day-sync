// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
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
                let bytes=match request.body {PreparedBody::Bytes(b)=>b.as_ref().clone(),_=>panic!("bounded bytes required")};
                if !path.contains("/tx-") && !s.fault_controls{s.files.insert(path,bytes);return Ok(Reply::new(201,vec![]));}
                s.puts+=1;let fault=std::mem::take(&mut s.fault);
                match fault {
                    Fault::Status(code)=>Ok(Reply::new(code,vec![]).header("Retry-After","120")),
                    Fault::FalseSuccess=>Ok(Reply::new(201,vec![])),
                    Fault::PartialPut=>{s.files.insert(path,bytes[..bytes.len()/2].to_vec());Err(http::HttpError::Connect)},
                    Fault::LostAck=>{s.files.insert(path,bytes);Err(http::HttpError::Connect)},
                    Fault::ContendedControl=>{s.files.insert(path,bytes);Ok(Reply::new(403,vec![]))},
                    _=>{s.files.insert(path,bytes);s.fault=fault;Ok(Reply::new(201,vec![]))}
                }
            },
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
                if !matches!(s.fault,Fault::IncompleteListing){xml+=&format!("<d:response><d:href>{path}</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");}
                if !zero{for name in s.files.keys().filter(|p|p.starts_with(&path)){xml+=&format!("<d:response><d:href>{name}</d:href><d:propstat><d:prop><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");}}
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
        assert!(matches!(
            a.sync(&client).await,
            Err(day_sync::Error::Store(e)) if e.kind == StoreErrorKind::IncompleteListing
        ));
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
        assert!(matches!(
            a.sync(&other).await,
            Err(day_sync::Error::WrongEndpoint)
        ));
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
            assert!(matches!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop())),
                Poll::Pending
            ));
            if !sim.requests().is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(!sim.requests().is_empty());
        assert!(matches!(a.sync(&client).await, Err(day_sync::Error::Busy)));
        drop(future);
        assert_eq!(a.snapshot().await.unwrap().pending_uploads, 1);
        sim.set_conditions(Default::default());
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
        assert!(matches!(
            a.sync(&client).await,
            Err(day_sync::Error::RemoteHistoryMissing)
        ));
        assert_eq!(a.snapshot().await.unwrap(), before);
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
        assert!(matches!(
            b.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { count: 1 })
        ));
        assert!(matches!(
            c.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { count: 1 })
        ));
        let snapshot = c.snapshot().await.unwrap();
        assert_eq!(snapshot.bookmarks.len(), 1); // The independent actor's record applied.
        assert_eq!(snapshot.waiting, 1); // The corrupt predecessor's successor did not.
        assert!(!snapshot.frontier.contains_key(a.actor()));
        assert_eq!(snapshot.frontier.get(b.actor()), Some(&1));
        assert!(!snapshot.poisoned);
        assert!(matches!(
            c.sync(&client).await,
            Err(day_sync::Error::RemoteObjectsRejected { .. })
        ));
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
