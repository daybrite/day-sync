//! Physical-device fixture: production engine, SQLite, and native HTTPS/WebDAV.
use day::prelude::*;
use day_sync::{Change, Identity, Replica, Space as SyncSpace, SyncOptions};
use day_sync_webdav::{WebDavStore, webdav::*};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Once},
    time::{Duration, Instant},
};

day::resources!();
day::day_start!(options: res::window_options(), root);

pub fn root() -> impl Piece {
    let lock = day_part_wakelock::keep_screen_on();
    day_reactive::Scope::current().on_cleanup(move || drop(lock));
    static START: Once = Once::new();
    START.call_once(|| {
        let config: Value = serde_json::from_slice(
            resource(res::assets::lab_json)
                .expect("generated lab configuration")
                .as_slice(),
        )
        .expect("valid lab configuration");
        let dir = day_process::storage::data_dir().expect("application data directory");
        std::thread::spawn(move || futures_lite::future::block_on(run(config, &dir)));
    });
    column((
        label(res::str::lab_running())
            .font(Font::Title)
            .id(day::id!(lab_title)),
        label(res::str::lab_body()).id(day::id!(lab_body)),
    ))
    .spacing(Space::L)
    .padding(Space::XL)
}

fn path(parts: &[&str]) -> ResourcePath {
    ResourcePath::from_segments(parts.iter().copied(), false).expect("fixture resource path")
}

fn persist(path: &Path, data: &[u8]) {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut file = std::fs::File::create(&tmp).expect("create lab journal");
    file.write_all(data).expect("write lab journal");
    file.sync_all().expect("flush lab journal");
    std::fs::rename(tmp, path).expect("replace lab journal");
}

async fn snapshot(r: &Replica) -> Value {
    match r.snapshot().await {
        Ok(s) => json!({
            "bookmarks": s.bookmarks.iter().map(|b| json!({"id":b.id,"url":b.url,"title":b.title})).collect::<Vec<_>>(),
            "candidates": s.candidates.iter().map(|c| json!({"id":c.id,"actor":c.actor,"sequence":c.sequence,"value":c.value})).collect::<Vec<_>>(),
            "frontier":s.frontier,"pending":s.pending_uploads,"waiting":s.waiting,
            "held":s.held,"poisoned":s.poisoned,"transactions":s.transactions
        }),
        Err(e) => json!({"error":format!("{e:?}")}),
    }
}

async fn run(cfg: Value, dir: &Path) {
    let role = if cfg!(target_os = "android") {
        "pixel"
    } else if cfg!(target_os = "ios") {
        "iphone"
    } else {
        "desktop"
    };
    std::fs::create_dir_all(dir).expect("create app data");
    let base = format!(
        "{}/{}/",
        cfg["url"].as_str().expect("url"),
        cfg["token"].as_str().expect("token")
    );
    let auth = Authentication::Basic {
        username: role.into(),
        password: cfg["password"].as_str().expect("password").into(),
    };
    let control = Client::builder(Endpoint::new(&base).expect("HTTPS controller"))
        .authentication(auth.clone())
        .build();
    let store = WebDavStore::new(
        Client::builder(Endpoint::new(&format!("{base}dav/")).expect("HTTPS DAV"))
            .authentication(auth)
            .build(),
    );
    let identity = Arc::new(Identity {
        space: SyncSpace {
            version: 5,
            encoding: "plain-json-v1".into(),
            id: cfg["space"].as_str().expect("space").into(),
            profile: "day-sync:bookmark:5".into(),
        },
        actor: cfg["actors"][role].as_str().expect("actor").into(),
    });
    let db = dir.join(format!("{}.db", identity.space.id));
    let mut replica = Replica::open(&db, identity.clone())
        .await
        .expect("open persistent replica");
    let journal = dir.join(format!("{}.report.json", identity.space.id));
    let mut report: Value = std::fs::read(&journal)
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_else(|| json!({"id":"boot","role":role,"snapshot":null}));
    let boot = format!("{:?}", std::time::SystemTime::now());
    loop {
        // Re-send the durable result after connection loss or process death; a lost result
        // never makes the controller silently advance to the next command.
        report["boot"] = json!(boot);
        if let Ok(response) = control
            .get(&path(&["control", &format!("{role}.json")]), None)
            .await
            && let Ok(cmd) = serde_json::from_slice::<Value>(&response.body)
            && cmd["id"] != report["id"]
            && cmd["op"] != "idle"
        {
            let start = Instant::now();
            persist(
                &dir.join("active-command.json"),
                &serde_json::to_vec(&cmd).expect("command JSON"),
            );
            let result = match cmd["op"].as_str().expect("command operation") {
                "commit" => {
                    let mut result = Ok(());
                    for changes in cmd["batches"].as_array().expect("batches") {
                        let edits: Vec<Change> =
                            serde_json::from_value(changes.clone()).expect("synthetic edits");
                        if let Err(e) = replica.commit(edits).await {
                            result = Err(e);
                            break;
                        }
                    }
                    result.map(|_| json!("committed"))
                }
                "sync" => replica.sync(&store).await.map(|r| json!(format!("{r:?}"))),
                "compact" => replica
                    .compact_with_options(
                        &store,
                        SyncOptions {
                            verify_snapshots: true,
                            max_deletions: cmd["deletions"].as_u64().unwrap_or(128) as usize,
                        },
                    )
                    .await
                    .map(|r| json!(format!("{r:?}"))),
                "reopen" => {
                    replica.close().await.expect("close replica");
                    replica = Replica::open(&db, identity.clone())
                        .await
                        .expect("reopen replica");
                    Ok(json!("reopened"))
                }
                "snapshot" => Ok(json!("observed")),
                "fresh" => {
                    let fresh_identity =
                        Arc::new(Identity::join(identity.space.clone()).expect("fresh identity"));
                    let fresh_db = dir.join(format!("fresh-{}.db", fresh_identity.actor));
                    persist(
                        &fresh_db.with_extension("identity.json"),
                        &fresh_identity.to_bytes().expect("identity JSON"),
                    );
                    let fresh = Replica::open(&fresh_db, fresh_identity)
                        .await
                        .expect("fresh database");
                    let outcome = match fresh.sync(&store).await {
                        Ok(_) => Ok(snapshot(&fresh).await),
                        Err(e) => Err(e),
                    };
                    fresh.close().await.expect("close fresh database");
                    outcome
                }
                _ => panic!("unknown lab command"),
            };
            report = json!({"id":cmd["id"],"role":role,"boot":boot,
                "elapsed_ms":start.elapsed().as_millis(),"ok":result.is_ok(),
                "result":result.unwrap_or_else(|e| json!({"error":format!("{e:?}")})),
                "snapshot":snapshot(&replica).await});
            persist(&journal, &serde_json::to_vec(&report).expect("report JSON"));
        }
        let bytes = serde_json::to_vec(&report).expect("report JSON");
        let _ = control
            .put(
                &path(&["reports", &format!("{role}.json")]),
                bytes,
                Condition::Unconditional,
            )
            .await;
        std::thread::sleep(Duration::from_millis(750));
    }
}
