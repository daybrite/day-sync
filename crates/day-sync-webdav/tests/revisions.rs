// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Devices at three application revisions share one space over WebDAV. The application is shaped
//! like Day-News's feed-state profile: revision 1 accepts exactly today's records, revision 2 adds
//! an `archived` kind, and revision 3 lets a star carry a note.
use day_persistence::{DbError, ModelContainer, Sqlite, SqliteConnection, Value, schema};
use day_sync::*;
use day_sync_webdav::{WebDavStore, webdav::*};
use futures_lite::future::block_on;
use serde_json::{Map, Value as Json, json};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc};

const PROFILE: &str = "day-sync-test:feed-state:1";
const FEED: &str = "https://feeds.example/news.xml";

struct News(u32);
fn invalid() -> DbError {
    DbError::driver("test.invalid_state")
}
fn state(change: &Change) -> Result<Map<String, Json>, DbError> {
    let value = change.value.as_ref().ok_or_else(invalid)?;
    if value.url != PROFILE {
        return Err(invalid());
    }
    match serde_json::from_str(&value.title).map_err(|_| invalid())? {
        Json::Object(m) => Ok(m),
        _ => Err(invalid()),
    }
}
/// The revision a record needs: an archive needs 2, a star with a note needs 3.
fn needed(m: &Map<String, Json>) -> u32 {
    if m.contains_key("note") {
        3
    } else if m["kind"] == "archived" {
        2
    } else {
        1
    }
}
impl Application for News {
    fn profile(&self) -> &'static str {
        PROFILE
    }
    fn install(&self, c: &mut dyn SqliteConnection) -> Result<(), DbError> {
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS items(id TEXT PRIMARY KEY,kind TEXT NOT NULL,guid TEXT NOT NULL,value INTEGER NOT NULL,note TEXT)",
        )
    }
    fn capture(&self, _: &mut dyn SqliteConnection) -> Result<Vec<Change>, DbError> {
        Ok(vec![])
    }
    fn validate(&self, change: &Change) -> Result<(), DbError> {
        // Strict like Day-News: unknown kinds or fields are invalid at a revision that predates them.
        let m = state(change)?;
        let mut kinds = vec!["subscription", "read", "star"];
        let mut fields = vec!["kind", "feed", "guid", "value", "label"];
        if self.0 >= 2 {
            kinds.push("archived");
        }
        if self.0 >= 3 {
            fields.push("note");
        }
        if m.keys().any(|k| !fields.contains(&k.as_str()))
            || !m["kind"].as_str().is_some_and(|k| kinds.contains(&k))
            || !m["value"].is_boolean()
            || (m.contains_key("note") && m["kind"] != "star")
            || needed(&m) > self.0
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn revision(&self) -> u32 {
        self.0
    }
    fn requires(&self, change: &Change) -> u32 {
        state(change).map(|m| needed(&m)).unwrap_or(1)
    }
    fn resolve(&self, values: &[Option<BookmarkValue>]) -> Result<usize, DbError> {
        // Day-News's rule: concurrent choices prefer unread, starred and unsubscribed.
        for (i, v) in values.iter().enumerate() {
            let m = state(&Change {
                id: String::new(),
                value: v.clone(),
            })?;
            if m["value"].as_bool() == Some(m["kind"] == "star") {
                return Ok(i);
            }
        }
        Ok(0)
    }
    fn project(
        &self,
        c: &mut dyn SqliteConnection,
        id: &str,
        value: Option<&BookmarkValue>,
    ) -> Result<(), DbError> {
        let m = state(&Change {
            id: id.into(),
            value: value.cloned(),
        })?;
        c.execute(
            "INSERT INTO items(id,kind,guid,value,note) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,guid=excluded.guid,value=excluded.value,note=excluded.note",
            &[
                Value::Text(id.into()),
                Value::Text(m["kind"].as_str().unwrap_or_default().into()),
                Value::Text(m["guid"].as_str().unwrap_or_default().into()),
                Value::Int((m["value"].as_bool() == Some(true)) as i64),
                m.get("note")
                    .and_then(Json::as_str)
                    .map_or(Value::Null, |n| Value::Text(n.into())),
            ],
        )?;
        Ok(())
    }
}
fn choice(kind: &str, guid: &str, value: bool, note: Option<&str>) -> Change {
    let mut m = json!({"kind": kind, "feed": FEED, "guid": guid, "value": value, "label": ""});
    if let Some(n) = note {
        m["note"] = json!(n);
    }
    Change {
        // Day-News's identity: one object per (kind, feed, guid), whatever its fields.
        id: Sha256::digest(serde_json::to_vec(&(PROFILE, kind, FEED, guid)).unwrap())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        value: Some(BookmarkValue {
            url: PROFILE.into(),
            title: m.to_string(),
        }),
    }
}
async fn device(path: &Path, identity: &Arc<Identity>, revision: u32) -> Replica {
    Replica::open_application(
        path,
        identity.clone(),
        Arc::new(|_| Ok(())),
        Some(Arc::new(News(revision))),
        |p| ModelContainer::open(Sqlite::at(p), schema![Bookmark]),
    )
    .await
    .unwrap()
}
/// What the device's own app tables show: (kind, guid) -> (value, note).
async fn items(r: &Replica) -> Vec<(String, String, bool, Option<String>)> {
    r.worker()
        .read(|db| {
            db.try_with_connection(|c| {
                let mut out = vec![];
                c.query(
                    "SELECT kind,guid,value,note FROM items ORDER BY kind,guid",
                    &[],
                    &mut |row| {
                        out.push((
                            row.get(0).as_text().unwrap_or_default().to_owned(),
                            row.get(1).as_text().unwrap_or_default().to_owned(),
                            row.get(2) == Value::Int(1),
                            row.get(3).as_text().ok().map(str::to_owned),
                        ))
                    },
                )?;
                Ok(out)
            })
        })
        .await
        .unwrap()
}
fn item(
    kind: &str,
    guid: &str,
    value: bool,
    note: Option<&str>,
) -> (String, String, bool, Option<String>) {
    (kind.into(), guid.into(), value, note.map(str::to_owned))
}
async fn object(client: &Client, folder: &str, prefix: &str) -> String {
    let listing = client
        .propfind(
            &ResourcePath::from_segments([folder], true).unwrap(),
            Depth::One,
        )
        .await
        .unwrap();
    let name = listing
        .resources
        .iter()
        .filter_map(|r| r.href.rsplit('/').next())
        .find(|n| n.starts_with(prefix))
        .unwrap()
        .to_owned();
    String::from_utf8(
        client
            .get(
                &ResourcePath::from_segments([folder, name.as_str()], false).unwrap(),
                None,
            )
            .await
            .unwrap()
            .body,
    )
    .unwrap()
}

fn scenario(client: WebDavStore) {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space_for(PROFILE).unwrap().space;
        let folder = format!("day-sync-v4-{}", space.id);
        let ids: Vec<Arc<Identity>> = (0..3)
            .map(|_| Arc::new(Identity::join(space.clone()).unwrap()))
            .collect();
        let (pa, pb, pc) = (
            dir.path().join("a.db"),
            dir.path().join("b.db"),
            dir.path().join("c.db"),
        );
        let a = device(&pa, &ids[0], 2).await;
        let b = device(&pb, &ids[1], 1).await;

        // A (revision 2) stars one article and archives another in ONE local commit. The journal
        // splits it: B can read the star, so only the archive needs revision 2.
        a.commit(vec![
            choice("star", "g1", true, None),
            choice("archived", "g2", true, None),
        ])
        .await
        .unwrap();
        a.sync(&client).await.unwrap();
        let a_records: Vec<String> = a
            .local_objects()
            .await
            .unwrap()
            .iter()
            .map(|o| String::from_utf8(o.bytes.clone()).unwrap())
            .collect();
        assert_eq!(a_records.len(), 2);
        assert!(
            a_records[0].contains(r#""version":4,"revision":2,"#),
            "{}",
            a_records[0]
        );
        assert!(
            a_records[1].contains(r#""version":4,"requires":2,"revision":2,"#),
            "{}",
            a_records[1]
        );
        println!("A revision-2 star record: {}", a_records[0]);
        println!("A revision-2 archive record: {}", a_records[1]);

        // B (revision 1) applies the star and holds the archive. Not an error.
        let report = b.sync(&client).await.unwrap();
        println!("B (r1) first sync: {report:?}");
        assert_eq!(
            (
                report.downloaded,
                report.applied,
                report.held,
                report.required_revision
            ),
            (2, 2, 1, 2)
        );
        assert_eq!(items(&b).await, vec![item("star", "g1", true, None)]);

        // B keeps syncing: its own revision-1 choice reaches A.
        b.commit(vec![choice("read", "g3", true, None)])
            .await
            .unwrap();
        b.sync(&client).await.unwrap();
        a.sync(&client).await.unwrap();
        assert_eq!(
            items(&a).await,
            vec![
                item("archived", "g2", true, None),
                item("read", "g3", true, None),
                item("star", "g1", true, None)
            ]
        );

        // C (revision 3) joins, replays everything, and adds a note to the star.
        let c = device(&pc, &ids[2], 3).await;
        let report = c.sync(&client).await.unwrap();
        assert_eq!((report.applied, report.held), (3, 0));
        c.commit(vec![choice("star", "g1", true, Some("read this weekend"))])
            .await
            .unwrap();
        c.sync(&client).await.unwrap();
        println!(
            "C revision-3 record: {}",
            object(client.client(), &folder, &format!("tx-{}.", c.actor())).await
        );

        // Meanwhile B, which cannot read C's note, unstars g1. Its revision-1 write does not
        // supersede the revision-3 value; it stays a concurrent candidate.
        b.commit(vec![choice("star", "g1", false, None)])
            .await
            .unwrap();
        let report = b.sync(&client).await.unwrap();
        println!("B (r1) after C's note: {report:?}");
        assert_eq!((report.held, report.required_revision), (2, 3));
        let report = a.sync(&client).await.unwrap();
        println!("A (r2) after C's note: {report:?}");
        assert_eq!((report.held, report.required_revision), (1, 3));
        // The older devices resolve only what they can read, so they agree with each other
        // whatever order the two concurrent records arrived in: B's unstar is all they can see.
        assert!(items(&b).await.contains(&item("star", "g1", false, None)));
        assert!(items(&a).await.contains(&item("star", "g1", false, None)));
        c.sync(&client).await.unwrap();
        // C sees both candidates and resolves them with the application's rule: starred wins.
        assert_eq!(
            items(&c).await,
            vec![
                item("archived", "g2", true, None),
                item("read", "g3", true, None),
                item("star", "g1", true, Some("read this weekend")),
            ]
        );
        let c_snapshot = c.snapshot().await.unwrap();
        assert_eq!(
            c_snapshot
                .candidates
                .iter()
                .filter(|x| x.id == choice("star", "g1", true, None).id)
                .count(),
            2
        );

        // A holds g1, so it refuses to take part in an epoch checkpoint until it is upgraded.
        // (Checked on a copy: freezing is irreversible.)
        a.close().await.unwrap();
        let frozen = dir.path().join("a-frozen.db");
        std::fs::copy(&pa, &frozen).unwrap();
        let copy = device(&frozen, &ids[0], 2).await;
        copy.freeze(&uuid::Uuid::new_v4().to_string())
            .await
            .unwrap();
        let refused = copy.attest().await.unwrap_err();
        assert!(
            format!("{refused:?}").contains("sync.checkpoint_requires_upgrade"),
            "{refused:?}"
        );
        copy.close().await.unwrap();

        // Upgrading releases the held objects locally, with no download, and every device converges.
        b.close().await.unwrap();
        let b = device(&pb, &ids[1], 3).await;
        let a = device(&pa, &ids[0], 3).await;
        for r in [&a, &b] {
            let report = r.sync(&client).await.unwrap();
            println!("upgraded {}: {report:?}", r.actor());
            assert_eq!((report.downloaded, report.held), (0, 0));
        }
        let expected = items(&c).await;
        for r in [&a, &b] {
            assert_eq!(items(r).await, expected);
            let s = r.snapshot().await.unwrap();
            assert_eq!(s.bookmarks, c_snapshot.bookmarks);
            assert_eq!(s.candidates, c_snapshot.candidates);
            assert_eq!(s.held, 0);
        }
        // Any revision-3 write on g1 now supersedes both candidates everywhere.
        b.commit(vec![choice("star", "g1", false, None)])
            .await
            .unwrap();
        b.sync(&client).await.unwrap();
        for r in [&a, &c] {
            r.sync(&client).await.unwrap();
            assert!(items(r).await.contains(&item("star", "g1", false, None)));
            assert_eq!(
                r.snapshot().await.unwrap().candidates,
                b.snapshot().await.unwrap().candidates
            );
        }
        for r in [a, b, c] {
            r.close().await.unwrap();
        }
        // The UUID namespace was freshly created by this test; never delete the supplied root.
        client
            .client()
            .delete(
                &ResourcePath::from_segments([folder], true).unwrap(),
                Condition::Unconditional,
            )
            .await
            .unwrap();
    });
}

#[test]
fn devices_at_three_revisions_hold_what_they_cannot_read_and_converge_after_upgrading() {
    let server = testing::Server::start().unwrap();
    scenario(WebDavStore::new(
        Client::builder(Endpoint::loopback(&server.url()).unwrap()).build(),
    ));
}
#[test]
#[ignore = "isolated Apache/Nextcloud fixture only"]
fn mixed_revisions_external() {
    let session = http::Session::new();
    #[cfg(feature = "reqwest")]
    if std::env::var_os("DAY_SYNC_REQWEST").is_some() {
        session.set_provider(http::Provider::Reqwest);
    }
    scenario(WebDavStore::new(
        Client::builder(Endpoint::loopback(&std::env::var("DAV_FIXTURE_URL").unwrap()).unwrap())
            .session(session)
            .authentication(Authentication::Basic {
                username: std::env::var("DAV_FIXTURE_USER")
                    .unwrap_or_else(|_| "day_fixture".into()),
                password: std::env::var("DAV_FIXTURE_PASSWORD").unwrap(),
            })
            .build(),
    ));
}

#[test]
fn held_objects_agree_at_one_revision_whatever_the_arrival_order() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let space = Identity::create_space_for(PROFILE).unwrap().space;
        let join = || Arc::new(Identity::join(space.clone()).unwrap());
        let (xi, yi, pi, qi) = (join(), join(), join(), join());
        let x = device(&dir.path().join("x.db"), &xi, 3).await;
        let y = device(&dir.path().join("y.db"), &yi, 1).await;
        let p = device(&dir.path().join("p.db"), &pi, 2).await;
        let q = device(&dir.path().join("q.db"), &qi, 2).await;
        async fn take(from: &Replica, to: &Replica, skip: usize) {
            for o in from.local_objects().await.unwrap().into_iter().skip(skip) {
                to.receive(o).await.unwrap();
                to.apply_ready().await.unwrap();
            }
        }
        // A common star, then a concurrent revision-3 note (X) and revision-1 unstar (Y).
        y.commit(vec![choice("star", "g1", true, None)])
            .await
            .unwrap();
        for r in [&x, &p, &q] {
            take(&y, r, 0).await;
        }
        x.commit(vec![choice("star", "g1", true, Some("later"))])
            .await
            .unwrap();
        y.commit(vec![choice("star", "g1", false, None)])
            .await
            .unwrap();
        take(&x, &p, 0).await;
        take(&y, &p, 1).await;
        take(&y, &q, 1).await;
        take(&x, &q, 0).await;
        assert_eq!(items(&p).await, items(&q).await);
        assert_eq!(items(&p).await, vec![item("star", "g1", false, None)]);
        for r in [&p, &q] {
            let s = r.snapshot().await.unwrap();
            assert_eq!((s.held, s.required_revision, s.candidates.len()), (1, 3, 2));
        }
        // After upgrading, both resolve the full set exactly as X does.
        take(&y, &x, 1).await;
        p.close().await.unwrap();
        q.close().await.unwrap();
        for (path, id) in [("p.db", &pi), ("q.db", &qi)] {
            let r = device(&dir.path().join(path), id, 3).await;
            r.apply_ready().await.unwrap();
            assert_eq!(items(&r).await, items(&x).await);
            assert_eq!(
                items(&r).await,
                vec![item("star", "g1", true, Some("later"))]
            );
            r.close().await.unwrap();
        }
        for r in [x, y] {
            r.close().await.unwrap();
        }
    });
}
