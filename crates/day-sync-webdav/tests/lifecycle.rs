// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![cfg(not(target_arch = "wasm32"))]
use day_sync::{lifecycle::*, *};
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
struct Store {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
    requests: Vec<(String, String)>,
    fail: Option<(usize, bool)>,
    corrupt: bool,
    delete_count: usize,
}
fn server() -> (WebDavStore, Arc<Mutex<Store>>) {
    let state = Arc::new(Mutex::new(Store::default()));
    let st = state.clone();
    let simulation = Simulation::new();
    simulation.handle(|_,_|true,move|r|{
  let mut s=st.lock().unwrap();let n=s.requests.len();s.requests.push((r.method.clone(),r.url.clone()));
  let fail=s.fail.filter(|(at,_)|*at==n);if fail.is_some(){s.fail=None;}
  if fail==Some((n,false)){return Err(http::HttpError::Connect);}
  let reply=match r.method.as_str(){
   "MKCOL"=>Reply::new(if s.dirs.insert(r.url.clone()){201}else{405},vec![]),
   "PUT"=>{let PreparedBody::Bytes(b)=r.body else{panic!("bounded body")};s.files.insert(r.url.clone(),b.to_vec());Reply::new(201,vec![])},
   "DELETE"=>{s.delete_count+=1;Reply::new(if s.files.remove(&r.url).is_some(){204}else{404},vec![])},
   "GET"=>{if let Some(mut bytes)=s.files.get(&r.url).cloned(){if s.corrupt{s.corrupt=false;bytes.push(0);}Reply::new(200,bytes)}else{Reply::new(404,vec![])}},
   "PROPFIND"=>{let mut xml=format!("<d:multistatus xmlns:d='DAV:'><d:response><d:href>{}</d:href><d:propstat><d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>",r.url);if !r.headers.iter().any(|(k,v)|k.eq_ignore_ascii_case("depth")&&v=="0"){for name in s.files.keys().filter(|p|p.starts_with(&r.url)){xml+=&format!("<d:response><d:href>{name}</d:href><d:propstat><d:prop><d:resourcetype/></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>");}}xml+="</d:multistatus>";Reply::new(207,xml.into_bytes())},
   _=>panic!("unexpected request")
  };
  if fail==Some((n,true)){return Err(http::HttpError::Connect);}Ok(reply)
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
fn change(id: usize, v: Option<&str>) -> Change {
    Change {
        id: format!("00000000-0000-4000-8000-{id:012}"),
        value: v.map(|t| BookmarkValue {
            url: "https://fixture.invalid".into(),
            title: t.into(),
        }),
    }
}
async fn cluster(dir: &std::path::Path, n: usize) -> (Vec<Replica>, Vec<Arc<Identity>>) {
    let key = Arc::new(Identity::create_space().unwrap());
    let mut rs = vec![Replica::open(dir.join("0.db"), key.clone()).await.unwrap()];
    let mut keys = vec![key];
    for i in 1..n {
        let k = Arc::new(Identity::join(rs[0].space_descriptor()).unwrap());
        rs.push(
            Replica::open(dir.join(format!("{i}.db")), k.clone())
                .await
                .unwrap(),
        );
        keys.push(k);
    }
    (rs, keys)
}
async fn converge(rs: &[Replica], client: &WebDavStore) {
    for _ in 0..3 {
        for r in rs {
            r.sync(client).await.unwrap();
        }
    }
    let s = rs[0].snapshot().await.unwrap();
    for r in rs {
        let v = r.snapshot().await.unwrap();
        assert_eq!(s.bookmarks, v.bookmarks);
        assert_eq!(s.candidates, v.candidates);
        assert_eq!(s.frontier, v.frontier);
        assert_eq!(v.pending_uploads, 0);
        assert_eq!(v.waiting, 0);
    }
}
async fn checkpoint(rs: &[Replica], client: &WebDavStore, target: &Replica) -> Package {
    let token = uuid::Uuid::new_v4().to_string();
    for r in rs {
        r.freeze(&token).await.unwrap();
    }
    converge(rs, client).await;
    let mut receipts = vec![];
    for r in rs {
        receipts.push(r.attest().await.unwrap());
    }
    rs[0]
        .prepare_checkpoint(target, receipts, vec![])
        .await
        .unwrap()
}
#[test]
fn stale_receipts_missing_members_and_explicit_retirement() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let (rs, _) = cluster(d.path(), 3).await;
        let (client, _) = server();
        converge(&rs, &client).await;
        let token = uuid::Uuid::new_v4().to_string();
        rs[0].freeze(&token).await.unwrap();
        rs[1].freeze(&token).await.unwrap();
        let before = rs[0].attest().await.unwrap();
        rs[2]
            .commit(vec![change(1, Some("offline unpublished"))])
            .await
            .unwrap();
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        assert!(
            rs[0]
                .prepare_checkpoint(
                    &target,
                    vec![before.clone(), rs[1].attest().await.unwrap()],
                    vec![]
                )
                .await
                .is_err()
        );
        rs[2].sync(&client).await.unwrap();
        rs[0].sync(&client).await.unwrap();
        rs[1].sync(&client).await.unwrap();
        assert!(
            rs[0]
                .prepare_checkpoint(
                    &target,
                    vec![before, rs[1].attest().await.unwrap()],
                    vec![rs[2].actor().into()]
                )
                .await
                .is_err()
        );
        let receipts = vec![rs[0].attest().await.unwrap(), rs[1].attest().await.unwrap()];
        let package = rs[0]
            .prepare_checkpoint(&target, receipts, vec![rs[2].actor().into()])
            .await
            .unwrap();
        // Retirement excludes future/unseen old-device writes. It is not automatic data salvage.
        rs[2]
            .commit(vec![change(2, Some("retired offline edit"))])
            .await
            .unwrap();
        package
            .publish(&client, &package.pin().unwrap())
            .await
            .unwrap();
        assert!(matches!(
            rs[2].sync(&client).await,
            Err(day_sync::Error::Retired)
        ));
        assert!(rs[0].sync(&client).await.is_err());
        assert_eq!(target.snapshot().await.unwrap().bookmarks.len(), 1);
        assert!(
            rs[2]
                .install_checkpoint(&package, &package.pin().unwrap())
                .await
                .is_err()
        );
        for r in rs {
            r.close().await.unwrap();
        }
        target.close().await.unwrap();
    });
}
#[test]
fn every_publication_boundary_can_retry_and_gc_survives_lost_delete_ack() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let (rs, _) = cluster(d.path(), 2).await;
        let (client, store) = server();
        rs[0]
            .commit(
                (0..300)
                    .map(|i| change(i, Some("Synthetic checkpoint")))
                    .collect(),
            )
            .await
            .unwrap();
        converge(&rs, &client).await;
        let key = Arc::new(Identity::create_space().unwrap());
        let target = Replica::open(d.path().join("target"), key.clone())
            .await
            .unwrap();
        let package = checkpoint(&rs, &client, &target).await;
        let pin = package.pin().unwrap();
        let (baseline, baseline_store) = server();
        package.publish(&baseline, &pin).await.unwrap();
        let count = baseline_store.lock().unwrap().requests.len();
        for after in [false, true] {
            for at in 0..count {
                let (faulty, state) = server();
                state.lock().unwrap().fail = Some((at, after));
                let _ = package.publish(&faulty, &pin).await;
                package.publish(&faulty, &pin).await.unwrap();
                Package::fetch(&faulty, &pin).await.unwrap();
            }
        }
        package.publish(&client, &pin).await.unwrap();
        store.lock().unwrap().corrupt = true;
        assert!(Package::fetch(&client, &pin).await.is_err());
        Package::fetch(&client, &pin).await.unwrap();
        let permit = Package::verify_backup(&package.export().unwrap(), &pin).unwrap();
        let reads = 1 + package.object_count();
        let original = store.lock().unwrap().clone();
        let deletes = rs[0].snapshot().await.unwrap().transactions as usize;
        for after in [false, true] {
            for offset in 0..reads + deletes {
                *store.lock().unwrap() = original.clone();
                store.lock().unwrap().fail = Some((original.requests.len() + offset, after));
                let _ = rs[0]
                    .collect_history(&client, &package, &pin, &permit)
                    .await;
                rs[0]
                    .collect_history(&client, &package, &pin, &permit)
                    .await
                    .unwrap();
                assert!(
                    store
                        .lock()
                        .unwrap()
                        .files
                        .keys()
                        .all(|p| !p.contains(&format!("day-sync-v4-{}/tx-", rs[0].space())))
                );
                Package::fetch(&client, &pin).await.unwrap();
            }
        }
        println!(
            "publication_boundaries={} publication_interruptions={} gc_boundaries={} gc_interruptions={}",
            count,
            count * 2,
            reads + deletes,
            (reads + deletes) * 2
        );
        for r in rs {
            r.close().await.unwrap();
        }
        target.close().await.unwrap();
    });
}
#[test]
fn freeze_survives_restart_and_import_failure_rolls_back_every_record() {
    block_on(async {
        use std::sync::atomic::{AtomicBool, Ordering};
        let d = tempfile::tempdir().unwrap();
        let (rs, keys) = cluster(d.path(), 2).await;
        let (client, _) = server();
        rs[0]
            .commit(
                (0..600)
                    .map(|i| change(i, Some("Synthetic atomic bootstrap")))
                    .collect(),
            )
            .await
            .unwrap();
        converge(&rs, &client).await;
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        let package = checkpoint(&rs, &client, &target).await;
        let pin = package.pin().unwrap();
        rs[1].close().await.unwrap();
        let reopened = Replica::open(d.path().join("1.db"), keys[1].clone())
            .await
            .unwrap();
        assert!(
            reopened
                .commit(vec![change(1, Some("forbidden thaw"))])
                .await
                .is_err()
        );
        let k = Arc::new(Identity::join(target.space_descriptor()).unwrap());
        let fail = Arc::new(AtomicBool::new(true));
        let f = fail.clone();
        let join = Replica::open_with_probe(
            d.path().join("join"),
            k,
            Arc::new(move |point| {
                if point == Checkpoint::BeforeApplyCommit && f.load(Ordering::SeqCst) {
                    Err(day_persistence::DbError::driver(
                        "synthetic checkpoint commit failure",
                    ))
                } else {
                    Ok(())
                }
            }),
        )
        .await
        .unwrap();
        assert!(join.install_checkpoint(&package, &pin).await.is_err());
        assert_eq!(join.snapshot().await.unwrap().transactions, 0);
        assert!(join.snapshot().await.unwrap().bookmarks.is_empty());
        fail.store(false, Ordering::SeqCst);
        join.install_checkpoint(&package, &pin).await.unwrap();
        assert_eq!(join.snapshot().await.unwrap().bookmarks.len(), 600);
        for r in rs {
            r.close().await.unwrap();
        }
        reopened.close().await.unwrap();
        target.close().await.unwrap();
        join.close().await.unwrap();
    });
}

#[test]
fn plaintext_checkpoint_roundtrip_bounds_recovery_and_fresh_writer() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let (rs, _) = cluster(d.path(), 3).await;
        let (client, _) = server();
        rs[0]
            .commit(
                (0..300)
                    .map(|i| change(i, Some("VISIBLE checkpoint fixture")))
                    .collect(),
            )
            .await
            .unwrap();
        converge(&rs, &client).await;
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        let package = checkpoint(&rs, &client, &target).await;
        let pin = package.pin().unwrap();
        let bytes = package.export().unwrap();
        assert!(
            bytes
                .windows(b"VISIBLE checkpoint fixture".len())
                .any(|v| v == b"VISIBLE checkpoint fixture")
        );
        let permit = Package::verify_backup(&bytes, &pin).unwrap();
        let mut bad = bytes.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(Package::from_bytes(&bad, &pin).is_err());
        let mut bad = bytes.clone();
        bad.push(0);
        assert!(Package::from_bytes(&bad, &pin).is_err());
        for n in [0, 8, 12, bytes.len() / 2, bytes.len() - 1] {
            assert!(Package::from_bytes(&bytes[..n], &pin).is_err());
        }
        let mut bad = bytes.clone();
        bad[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(Package::from_bytes(&bad, &pin).is_err());
        package.publish(&client, &pin).await.unwrap();
        assert!(matches!(
            Space::fetch(&client, rs[0].space()).await,
            Err(day_sync::Error::Retired)
        ));
        let fetched = Package::fetch(&client, &pin).await.unwrap();
        let join = Replica::open(
            d.path().join("join"),
            Arc::new(Identity::join(fetched.destination(&pin).unwrap()).unwrap()),
        )
        .await
        .unwrap();
        join.install_checkpoint(&fetched, &pin).await.unwrap();
        join.commit(vec![change(400, Some("new"))]).await.unwrap();
        join.install_checkpoint(&fetched, &pin).await.unwrap();
        assert_eq!(join.snapshot().await.unwrap().bookmarks.len(), 301);
        let recovered = fetched
            .recover(&pin, RecoveryPolicy::AcceptLossAfterCheckpoint)
            .unwrap();
        assert_ne!(recovered.identity.space(), target.space());
        let restore = Replica::open(d.path().join("restore"), recovered.identity.clone())
            .await
            .unwrap();
        recovered.seed(&restore).await.unwrap();
        recovered.seed(&restore).await.unwrap();
        assert_eq!(restore.snapshot().await.unwrap().bookmarks.len(), 300);
        rs[0]
            .collect_history(&client, &package, &pin, &permit)
            .await
            .unwrap();
        assert!(matches!(
            rs[2].sync(&client).await,
            Err(day_sync::Error::Retired)
        ));
        for r in rs {
            r.close().await.unwrap();
        }
        target.close().await.unwrap();
        join.close().await.unwrap();
        restore.close().await.unwrap();
    });
}
#[test]
fn local_schema_version_prevents_legacy_fence_bypass() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let key = Arc::new(Identity::create_space().unwrap());
        let path = d.path().join("current.db");
        let replica = Replica::open(&path, key.clone()).await.unwrap();
        replica
            .freeze(&uuid::Uuid::new_v4().to_string())
            .await
            .unwrap();
        replica.close().await.unwrap();
        let raw = rusqlite::Connection::open(&path).unwrap();
        let version: i64 = raw
            .query_row("SELECT version FROM _day_sync_config", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 4);
        assert_ne!(
            version, 2,
            "the legacy opener requires version 2 and must refuse this store"
        );
        drop(raw);
        let legacy = d.path().join("legacy.db");
        let raw = rusqlite::Connection::open(&legacy).unwrap();
        raw.execute_batch("CREATE TABLE _day_sync_config(id INTEGER PRIMARY KEY,version INTEGER,space TEXT,actor TEXT);INSERT INTO _day_sync_config VALUES(1,2,'synthetic','synthetic');").unwrap();
        drop(raw);
        assert!(Replica::open(&legacy, key).await.is_err());
        let raw = rusqlite::Connection::open(&legacy).unwrap();
        let version: i64 = raw
            .query_row("SELECT version FROM _day_sync_config", [], |r| r.get(0))
            .unwrap();
        assert_eq!(version, 2, "rejection must not migrate the old store");
    });
}

#[test]
#[ignore = "subprocess entry; parent supplies a disposable fixture"]
fn plaintext_lifecycle_child() {
    block_on(async {
        let dir = std::path::PathBuf::from(std::env::var("PLAIN_SYNC_CRASH_DIR").unwrap());
        let stage = std::env::var("PLAIN_SYNC_CRASH_STAGE").unwrap();
        let identity = Arc::new(
            Identity::from_bytes(&std::fs::read(dir.join("identity.json")).unwrap()).unwrap(),
        );
        let point = stage.clone();
        let replica = Replica::open_with_probe(
            dir.join("client.db"),
            identity.clone(),
            Arc::new(move |p| {
                if (point == "import-before" && p == Checkpoint::BeforeApplyCommit)
                    || (point == "recovery-before" && p == Checkpoint::BeforeJournal)
                    || (point == "recovery-journal" && p == Checkpoint::AfterJournal)
                    || (point == "recovery-after" && p == Checkpoint::LocalCommitted)
                {
                    std::process::exit(86);
                }
                Ok(())
            }),
        )
        .await
        .unwrap();
        if stage == "freeze" {
            replica
                .freeze("aaaaaaaa-aaaa-4aaa-8aaa-000000000001")
                .await
                .unwrap();
            std::process::exit(86);
        }
        let pin: CheckpointPin =
            serde_json::from_slice(&std::fs::read(dir.join("pin.json")).unwrap()).unwrap();
        let package =
            Package::from_bytes(&std::fs::read(dir.join("package.bin")).unwrap(), &pin).unwrap();
        if stage.starts_with("recovery-") {
            package
                .recover_to(&pin, RecoveryPolicy::AcceptLossAfterCheckpoint, identity)
                .unwrap()
                .seed(&replica)
                .await
                .unwrap();
        } else {
            replica.install_checkpoint(&package, &pin).await.unwrap();
        }
        assert_eq!(stage, "import-after");
        std::process::exit(86);
    });
}
#[test]
fn process_death_during_freeze_import_and_recovery_preserves_atomicity() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let (rs, _) = cluster(d.path(), 2).await;
        let (client, _) = server();
        rs[0]
            .commit(
                (0..300)
                    .map(|i| change(i, Some("crash checkpoint fixture")))
                    .collect(),
            )
            .await
            .unwrap();
        converge(&rs, &client).await;
        let target = Replica::open(
            d.path().join("target"),
            Arc::new(Identity::create_space().unwrap()),
        )
        .await
        .unwrap();
        let package = checkpoint(&rs, &client, &target).await;
        let pin = package.pin().unwrap();
        for stage in [
            "freeze",
            "import-before",
            "import-after",
            "recovery-before",
            "recovery-journal",
            "recovery-after",
        ] {
            let sub = d.path().join(stage);
            std::fs::create_dir(&sub).unwrap();
            let identity = Arc::new(if stage.starts_with("import-") {
                Identity::join(target.space_descriptor()).unwrap()
            } else {
                Identity::create_space().unwrap()
            });
            std::fs::write(sub.join("identity.json"), identity.to_bytes().unwrap()).unwrap();
            std::fs::write(sub.join("pin.json"), serde_json::to_vec(&pin).unwrap()).unwrap();
            std::fs::write(sub.join("package.bin"), package.export().unwrap()).unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "plaintext_lifecycle_child", "--ignored"])
                .env("PLAIN_SYNC_CRASH_DIR", &sub)
                .env("PLAIN_SYNC_CRASH_STAGE", stage)
                .output()
                .unwrap();
            assert_eq!(
                status.status.code(),
                Some(86),
                "{stage}: {}",
                String::from_utf8_lossy(&status.stderr)
            );
            let r = Replica::open(sub.join("client.db"), identity.clone())
                .await
                .unwrap();
            if stage == "freeze" {
                assert!(
                    r.commit(vec![change(1, Some("must remain frozen"))])
                        .await
                        .is_err()
                );
            } else {
                assert_eq!(
                    r.snapshot().await.unwrap().bookmarks.len(),
                    if stage.ends_with("after") { 300 } else { 0 }
                );
                if stage.starts_with("recovery-") {
                    let recovery = package
                        .recover_to(&pin, RecoveryPolicy::AcceptLossAfterCheckpoint, identity)
                        .unwrap();
                    recovery.seed(&r).await.unwrap();
                    r.commit(vec![change(400, Some("post recovery"))])
                        .await
                        .unwrap();
                    recovery.seed(&r).await.unwrap();
                    assert_eq!(r.snapshot().await.unwrap().bookmarks.len(), 301);
                } else {
                    r.install_checkpoint(&package, &pin).await.unwrap();
                    assert_eq!(r.snapshot().await.unwrap().bookmarks.len(), 300);
                }
            }
            r.close().await.unwrap();
        }
        for r in rs {
            r.close().await.unwrap();
        }
        target.close().await.unwrap();
    });
}
