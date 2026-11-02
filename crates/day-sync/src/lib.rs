// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Experimental plaintext replication over authenticated WebDAV/HTTPS.
//! The server and every authorized writer are trusted. Hashes are corruption checks, not signatures.
//! A versioned record boundary reserves room for a future separately reviewed protected format.
#![cfg(not(target_arch = "wasm32"))]
mod application;
pub mod lifecycle;
mod membership;
pub use application::Application;
mod envelope;
mod identity;
mod storage;
pub use identity::{Identity, Space};
mod wire;
use day_persistence::{
    DatabaseWorker, DbError, Model, ModelContainer, Sqlite, Value, WriteOrigin, schema,
};
pub mod store;
use serde::{Deserialize, Serialize};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use storage::{config, error, frontier, healthy, int, number, rows, text};
pub use store::{Created, MemoryStore, Store, StoreError, StoreErrorKind, StoreFuture};
use wire::digest;

#[derive(Model, Clone, Default, Debug, PartialEq, Eq)]
#[model(table = "sync_bookmarks")]
pub struct Bookmark {
    #[model(id)]
    pub id: String,
    pub url: String,
    pub title: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BookmarkValue {
    pub url: String,
    pub title: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Change {
    pub id: String,
    pub value: Option<BookmarkValue>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidate {
    pub id: String,
    pub actor: String,
    pub sequence: u64,
    pub value: Option<BookmarkValue>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Object {
    pub filename: String,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub uploaded: usize,
    pub downloaded: usize,
    pub applied: usize,
    pub waiting: usize,
    /// Objects kept at their last readable value because another device wrote a record revision
    /// this build cannot read. Not an error: everything else keeps syncing.
    pub held: usize,
    /// The record revision a build needs to read every held object; 0 when nothing is held.
    pub required_revision: u32,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub bookmarks: Vec<Bookmark>,
    pub candidates: Vec<Candidate>,
    pub frontier: std::collections::BTreeMap<String, u64>,
    pub transactions: u64,
    pub pending_uploads: u64,
    pub waiting: u64,
    pub held: u64,
    pub required_revision: u32,
    pub poisoned: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checkpoint {
    BeforeJournal,
    AfterJournal,
    LocalCommitted,
    BeforeUpload,
    Uploaded,
    Verified,
    BeforePublishReceipt,
    Published,
    InboxStored,
    BeforeApplyCommit,
    Applied,
}
/// Experimental fault instrumentation. Returning an error aborts work at this point; after a
/// commit it does NOT undo that commit. Process-crash tests terminate at these exact boundaries.
pub type Probe = Arc<dyn Fn(Checkpoint) -> Result<(), DbError> + Send + Sync>;
#[derive(Debug)]
pub enum Error {
    Database(DbError),
    Store(StoreError),
    Busy,
    InvalidWire,
    Integrity,
    Fork,
    WrongEndpoint,
    Limit,
    RemoteHistoryMissing,
    /// Some remote objects failed validation; independent valid work may have committed.
    RemoteObjectsRejected {
        count: usize,
    },
    WrongSpace,
    UnsupportedFormat,
    Retired,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Database(_) => "sync.database",
            Self::Store(_) => "sync.store",
            Self::Busy => "sync.busy",
            Self::InvalidWire => "sync.invalid_wire",
            Self::Integrity => "sync.integrity",
            Self::Fork => "sync.fork",
            Self::WrongEndpoint => "sync.endpoint_changed",
            Self::Limit => "sync.limit",
            Self::RemoteHistoryMissing => "sync.remote_history_missing",
            Self::RemoteObjectsRejected { .. } => "sync.remote_objects_rejected",
            Self::WrongSpace => "sync.wrong_space",
            Self::UnsupportedFormat => "sync.unsupported_format",
            Self::Retired => "sync.epoch_retired",
        })
    }
}
impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Database(e) => Some(e),
            Self::Store(e) => Some(e),
            _ => None,
        }
    }
}
impl From<DbError> for Error {
    fn from(e: DbError) -> Self {
        Self::Database(e)
    }
}
impl From<StoreError> for Error {
    fn from(e: StoreError) -> Self {
        Self::Store(e)
    }
}
#[derive(Clone)]
pub struct Replica {
    identity: Arc<Identity>,
    db: DatabaseWorker,
    space: String,
    actor: String,
    probe: Probe,
    running: Arc<AtomicBool>,
    application: Option<Arc<dyn Application>>,
}
struct RunGuard(Arc<AtomicBool>);
impl Drop for RunGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl Replica {
    /// Open a v4 plaintext store. Persist public identity alongside the database. Older formats
    /// are rejected; an old encrypted space is never silently converted.
    pub async fn open(path: impl AsRef<Path>, identity: Arc<Identity>) -> Result<Self, Error> {
        Self::open_with_probe(path, identity, Arc::new(|_| Ok(()))).await
    }
    pub async fn open_with_probe(
        path: impl AsRef<Path>,
        identity: Arc<Identity>,
        probe: Probe,
    ) -> Result<Self, Error> {
        Self::open_application(path, identity, probe, None, |path| {
            ModelContainer::open(Sqlite::at(path), schema![Bookmark])
        })
        .await
    }
    /// Open an application's database and install the journal before any writers can access it.
    /// The factory must register Bookmark (the current bounded projection) alongside its own models.
    pub async fn open_application(
        path: impl AsRef<Path>,
        identity: Arc<Identity>,
        probe: Probe,
        application: Option<Arc<dyn Application>>,
        factory: impl FnOnce(std::path::PathBuf) -> Result<ModelContainer, DbError> + Send + 'static,
    ) -> Result<Self, Error> {
        identity.validate()?;
        let space = identity.space();
        let path = path.as_ref();
        let path = if path.exists() {
            path.canonicalize()
        } else {
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("."))
                .canonicalize()
                .map(|p| p.join(path.file_name().unwrap_or_default()))
        }
        .map_err(|_| error("sync.invalid_path"))?;
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".day-sync-lock");
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)
            .map_err(|_| error("sync.lock_file"))?;
        lease.try_lock().map_err(|_| Error::Busy)?;
        let space = space.to_owned();
        let init_identity = identity.clone();
        let hook = probe.clone();
        let adapter = application.clone();
        let db = DatabaseWorker::open(move || {
            let db = factory(path)?;
            db.try_with_connection(|c| storage::install(c, &init_identity))?;
            db.try_with_connection(|c| {
                c.begin()?;
                let result = (|| {
                    let name = adapter
                        .as_ref()
                        .map(|a| a.profile())
                        .unwrap_or("day-sync:bookmark:4");
                    if name != init_identity.space.profile {
                        return Err(error("sync.wrong_application_profile"));
                    }
                    let saved = rows(c, "SELECT name FROM _day_sync_profile WHERE id=1", &[])?;
                    if let Some(row) = saved.first() {
                        if row[0].as_text()? != name {
                            return Err(error("sync.wrong_application_profile"));
                        }
                    } else {
                        if !rows(c, "SELECT id FROM sync_bookmarks LIMIT 1", &[])?.is_empty() {
                            return Err(error("sync.unversioned_application_profile"));
                        }
                        c.execute("INSERT INTO _day_sync_profile VALUES(1,?)", &[text(name)])?;
                    }
                    if let Some(adapter) = &adapter {
                        adapter.install(c)?;
                    }
                    c.commit()
                })();
                if result.is_err() {
                    let _ = c.rollback();
                }
                result
            })?;
            db.install_transaction_participant(storage::Journal {
                application: adapter,
                identity: init_identity,
                probe: hook,
                _lease: lease,
            })?;
            Ok(db)
        })
        .await?;
        let actor = db
            .read(|db| db.try_with_connection(|c| Ok(config(c)?[1].as_text()?.to_owned())))
            .await?;
        Ok(Self {
            identity,
            db,
            space,
            actor,
            probe,
            running: Arc::new(AtomicBool::new(false)),
            application,
        })
    }
    /// Shares the same transactional writer as replication; never open a second writer.
    pub fn worker(&self) -> DatabaseWorker {
        self.db.clone()
    }
    pub fn space_descriptor(&self) -> Space {
        self.identity.space.clone()
    }
    pub fn actor(&self) -> &str {
        &self.actor
    }
    pub fn space(&self) -> &str {
        &self.space
    }
    /// One local logical transaction, including explicit deletes of absent objects. An explicit
    /// choice of the current value also resolves other observed concurrent candidates.
    pub async fn commit(&self, changes: Vec<Change>) -> Result<(), Error> {
        self.commit_inner(changes, None).await
    }
    async fn commit_inner(
        &self,
        mut changes: Vec<Change>,
        recovery: Option<String>,
    ) -> Result<(), Error> {
        // Local batches may contain many bounded wire records, all committed atomically.
        // Keep a separate admission bound; the journal still enforces per-record limits.
        if (changes.is_empty() && recovery.is_none()) || changes.len() > 65_536 {
            return Err(Error::Limit);
        }
        changes.sort_by(|a, b| a.id.cmp(&b.id));
        if changes.windows(2).any(|w| w[0].id == w[1].id)
            || changes.iter().any(|c| {
                !wire::valid_object(&c.id)
                    || c.value.as_ref().is_some_and(|v| !wire::validate_value(v))
            })
        {
            return Err(Error::InvalidWire);
        }
        self.db
            .write(move |db| {
                if let Some(hash) = recovery {
                    let installed = db.try_with_connection(|c| {
                        if let Some(row) =
                            rows(c, "SELECT hash FROM _day_sync_recovery WHERE id=1", &[])?.first()
                        {
                            return if row[0].as_text()? == hash {
                                Ok(true)
                            } else {
                                Err(error("sync.recovery_fork"))
                            };
                        }
                        if !rows(c, "SELECT actor FROM _day_sync_transactions LIMIT 1", &[])?
                            .is_empty()
                            || !rows(c, "SELECT object FROM _day_sync_candidates LIMIT 1", &[])?
                                .is_empty()
                        {
                            return Err(error("sync.recovery_requires_empty"));
                        }
                        c.execute("INSERT INTO _day_sync_recovery VALUES(1,?)", &[text(hash)])?;
                        Ok(false)
                    })?;
                    if installed {
                        return Ok(());
                    }
                }
                for change in changes {
                    if let Some(v) = change.value {
                        // insert's keyed collection replaces a matching identity; materialize first
                        // so the existing row and its update participate in normal persistence.
                        db.try_get::<Bookmark>(change.id.clone())?;
                        db.insert(Bookmark {
                            id: change.id.clone(),
                            url: v.url,
                            title: v.title,
                        });
                    } else {
                        db.delete::<Bookmark>(change.id.clone())?;
                    }
                    db.try_with_connection(|c| {
                        c.execute(
                            "INSERT OR IGNORE INTO _day_sync_dirty(object) VALUES(?)",
                            &[text(change.id)],
                        )?;
                        Ok(())
                    })?;
                }
                Ok(())
            })
            .await?;
        (self.probe)(Checkpoint::LocalCommitted)?;
        Ok(())
    }
    pub async fn snapshot(&self) -> Result<Snapshot, Error> {
        Ok(self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    let bookmarks = rows(
                        c,
                        "SELECT id,url,title FROM sync_bookmarks ORDER BY id",
                        &[],
                    )?
                    .into_iter()
                    .map(|r| {
                        Ok(Bookmark {
                            id: r[0].as_text()?.into(),
                            url: r[1].as_text()?.into(),
                            title: r[2].as_text()?.into(),
                        })
                    })
                    .collect::<Result<_, DbError>>()?;

                    let candidates = rows(
                        c,
                        concat!(
                            "SELECT object,actor,sequence,value FROM ",
                            "_day_sync_candidates ORDER BY object,actor,sequence"
                        ),
                        &[],
                    )?
                    .into_iter()
                    .map(|r| {
                        Ok(Candidate {
                            id: r[0].as_text()?.into(),
                            actor: r[1].as_text()?.into(),
                            sequence: number(&r[2])?,
                            value: if r[3] == Value::Null {
                                None
                            } else {
                                Some(
                                    serde_json::from_slice(r[3].as_blob()?)
                                        .map_err(|_| error("sync.invalid_candidate"))?,
                                )
                            },
                        })
                    })
                    .collect::<Result<_, DbError>>()?;

                    let counts = rows(
                        c,
                        concat!(
                            "SELECT count(*),coalesce(sum(local=1 AND ",
                            "published=0),0),coalesce(sum(applied=0),0) FROM _day_sync_transactions"
                        ),
                        &[],
                    )?
                    .remove(0);
                    let (held, required_revision) = storage::held(c)?;
                    Ok(Snapshot {
                        bookmarks,
                        candidates,
                        frontier: frontier(c)?,
                        transactions: number(&counts[0])?,
                        pending_uploads: number(&counts[1])?,
                        waiting: number(&counts[2])?,
                        held,
                        required_revision,
                        poisoned: number(&config(c)?[5])? != 0,
                    })
                })
            })
            .await?)
    }
    /// Export immutable locally authored objects for diagnostics / deterministic transport tests.
    pub async fn local_objects(&self) -> Result<Vec<Object>, Error> {
        self.objects(false).await
    }
    async fn objects(&self, pending: bool) -> Result<Vec<Object>, Error> {
        let identity = self.identity.clone();
        Ok(self
            .db
            .read(move |db| {
                db.try_with_connection(|c| {
                    healthy(c)?;
                    rows(
                        c,
                        if pending {
                            concat!(
                                concat!(
                                    "SELECT hash,bytes FROM _day_sync_transactions WHERE local=1 ",
                                    "AND published=0 "
                                ),
                                "ORDER BY sequence LIMIT 1"
                            )
                        } else {
                            concat!(
                                "SELECT hash,bytes FROM _day_sync_transactions WHERE local=1 ",
                                "ORDER BY sequence"
                            )
                        },
                        &[],
                    )?
                    .into_iter()
                    .map(|r| {
                        let bytes = r[1].as_blob()?.to_vec();
                        let hash = r[0].as_text()?;
                        if digest(&bytes) != hash {
                            return Err(error("sync.local_integrity"));
                        }
                        let tx = identity
                            .decode_record(&bytes)
                            .map_err(|_| error("sync.local_wire"))?;
                        Ok(Object {
                            filename: tx.filename(hash),
                            bytes,
                        })
                    })
                    .collect()
                })
            })
            .await?)
    }
    /// Persist a complete verified object before applying anything. Returns false for an exact
    /// duplicate. A reused actor/sequence with different bytes permanently quarantines the store.
    pub async fn receive(&self, object: Object) -> Result<bool, Error> {
        if object.bytes.len() > envelope::MAX_RECORD {
            return Err(Error::Limit);
        }
        // Hashing, format decoding and application validation run on
        // the database queue, not on a caller's main-loop executor. Return the already-owned
        // bytes with the verified record so no second record copy crosses the queue.
        let identity = self.identity.clone();
        let application = self.application.clone();
        let (object, actor, sequence, hash, tx) = self
            .db
            .read(move |_db| {
                Ok((|| -> Result<_, Error> {
                    let (actor, sequence, hash) = wire::parse_filename(&object.filename)?;
                    if digest(&object.bytes) != hash {
                        return Err(Error::Integrity);
                    }
                    let tx = identity.decode_record(&object.bytes)?;
                    if tx.actor != actor || tx.sequence != sequence {
                        return Err(Error::WrongSpace);
                    }
                    // A newer revision is stored unread and validated when a build that
                    // reads it releases the objects it touches.
                    if let Some(app) = application.filter(|app| tx.requires <= app.revision()) {
                        for change in &tx.changes {
                            app.validate(change)?;
                        }
                    }
                    Ok((object, actor, sequence, hash, tx))
                })())
            })
            .await??;
        let probe = self.probe.clone();
        let local_identity = self.identity.clone();
        let sender = Identity {
            space: self.identity.space.clone(),
            actor: actor.clone(),
        }
        .to_bytes()?;
        let result = self
            .db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    healthy(c)?;
                    if !rows(c, "SELECT id FROM _day_sync_handoff", &[])?.is_empty() {
                        return Err(error("sync.epoch_retired"));
                    }
                    let existing = rows(
                        c,
                        concat!(
                            "SELECT hash FROM _day_sync_transactions WHERE actor=? AND ",
                            "sequence=?"
                        ),
                        &[text(&actor), int(sequence)],
                    )?;
                    if let Some(r) = existing.first() {
                        if r[0].as_text()? != hash {
                            c.execute(
                                concat!(
                                    "INSERT OR IGNORE INTO ",
                                    "_day_sync_forks(hash,actor,sequence,bytes) VALUES(?,?,?,?)"
                                ),
                                &[
                                    text(&hash),
                                    text(&actor),
                                    int(sequence),
                                    Value::Blob(object.bytes.clone()),
                                ],
                            )?;
                            c.execute("UPDATE _day_sync_config SET poisoned=1 WHERE id=1", &[])?;
                            return Ok(2);
                        }

                        return Ok(0);
                    }
                    // A clone may advertise a NEW sequence for our own actor before this copy wrote it.
                    // Never import it and then issue the same local sequence from a stale counter.
                    if config(c)?[1].as_text()? == actor {
                        c.execute(
                            concat!(
                                "INSERT OR IGNORE INTO ",
                                "_day_sync_forks(hash,actor,sequence,bytes) VALUES(?,?,?,?)"
                            ),
                            &[
                                text(&hash),
                                text(&actor),
                                int(sequence),
                                Value::Blob(object.bytes.clone()),
                            ],
                        )?;
                        c.execute("UPDATE _day_sync_config SET poisoned=1 WHERE id=1", &[])?;
                        return Ok(2);
                    }

                    storage::insert_member(c, &sender, &local_identity)?;
                    storage::reserve(c, &tx, &hash, &object.bytes, false)?;
                    Ok(1)
                })
            })
            .await?;
        if result == 2 {
            return Err(Error::Fork);
        }
        if result == 1 {
            probe(Checkpoint::InboxStored)?;
        }
        Ok(result == 1)
    }
    /// Drain causally ready inbox entries; unresolved predecessors remain durably queued.
    pub async fn apply_ready(&self) -> Result<usize, Error> {
        let mut applied = 0;
        // An upgraded build first projects the objects an older build held for it. Read first:
        // the common case writes nothing and passes no apply boundary.
        let revision = storage::reader(self.application.as_deref());
        let releasable = self
            .db
            .read(move |db| {
                db.try_with_connection(|c| {
                    Ok(!rows(
                        c,
                        "SELECT object FROM _day_sync_held WHERE requires<=? LIMIT 1",
                        &[int(revision.into())],
                    )?
                    .is_empty())
                })
            })
            .await?;
        let adapter = self.application.clone();
        if releasable {
            self.db
                .write_with_origin(WriteOrigin::RemoteImport, move |db| {
                    let released = db.try_with_connection(|c| {
                        healthy(c)?;
                        storage::release_held(c, adapter.as_deref())
                    })?;
                    if released != 0 {
                        db.rescan()?;
                    }
                    Ok(())
                })
                .await?;
        }
        loop {
            let identity = self.identity.clone();
            let adapter = self.application.clone();
            let changed =
                self.db
                    .write_with_origin(WriteOrigin::RemoteImport, move |db| {
                        let changed = db.try_with_connection(|c| {
                            healthy(c)?;
                            // Lamport order presents predecessors first during a fresh bootstrap. Actor
                            // order repeatedly decoded a large blocked suffix on every apply step.
                            // This is an optimization only: ready() still checks dependency closure.
                            let pending = rows(
                                c,
                                concat!(
                                    "SELECT actor,sequence FROM _day_sync_transactions WHERE ",
                                    "applied=0 ORDER BY lamport,actor,sequence"
                                ),
                                &[],
                            )?;
                            for key in pending {
                                let r = rows(c,concat!(
"SELECT hash,bytes FROM _day_sync_transactions WHERE actor=? ",
"AND sequence=?"
),&key)?.remove(0);
                                let bytes = r[1].as_blob()?;
                                if digest(bytes) != r[0].as_text()? {
                                    return Err(error("sync.inbox_integrity"));
                                }
                                let tx = identity
                                    .decode_record(bytes)
                                    .map_err(|_| error("sync.inbox_wire"))?;
                                if storage::ready(c, &tx, &identity)? {
                                    storage::merge(c, &tx, true, adapter.as_deref())?;
                                    return Ok(true);
                                }
                            }
                            Ok(false)
                        })?;
                        if changed {
                            db.rescan()?;
                        }
                        Ok(changed)
                    })
                    .await?;
            if !changed {
                break;
            }
            applied += 1;
            (self.probe)(Checkpoint::Applied)?;
        }
        Ok(applied)
    }
    /// Synchronize once against `store`. One active run per Replica family; cancellation
    /// releases admission. Database jobs already accepted continue atomically even if the
    /// caller drops this future.
    pub async fn sync(&self, store: &dyn Store) -> Result<SyncReport, Error> {
        if self.running.swap(true, Ordering::Acquire) {
            return Err(Error::Busy);
        }
        let _guard = RunGuard(self.running.clone());
        let endpoint = store.endpoint();
        let same = self
            .db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    healthy(c)?;
                    let cfg = config(c)?;
                    if cfg[4] != Value::Null && cfg[4].as_text()? != endpoint {
                        return Ok(false);
                    }
                    c.execute(
                        "UPDATE _day_sync_config SET endpoint=? WHERE id=1",
                        &[text(endpoint)],
                    )?;
                    Ok(true)
                })
            })
            .await?;
        if !same {
            return Err(Error::WrongEndpoint);
        }
        let directory = vec![format!("day-sync-v4-{}", self.space)];
        store.ensure_directory(&directory).await?;
        self.sync_members(store, &directory).await?;
        let mut report = SyncReport::default();
        loop {
            let Some(object) = self.objects(true).await?.pop() else {
                break;
            };
            // Reconcile first: a lost write acknowledgement must not force a second upload.
            let present = store.get(&directory, &object.filename).await?.as_deref()
                == Some(object.bytes.as_slice());
            if !present {
                (self.probe)(Checkpoint::BeforeUpload)?;
                // Only our persisted content-addressed object. Repair partial bytes under this
                // name with exactly the same bytes; correctness does not rely on a conditional
                // write.
                store
                    .put(&directory, &object.filename, object.bytes.clone())
                    .await?;
                (self.probe)(Checkpoint::Uploaded)?;
                if store.get(&directory, &object.filename).await?.as_deref()
                    != Some(object.bytes.as_slice())
                {
                    return Err(Error::Integrity);
                }
            }
            (self.probe)(Checkpoint::Verified)?;
            let (_, _, hash) = wire::parse_filename(&object.filename)?;
            let probe = self.probe.clone();
            self.db
                .write_with_origin(WriteOrigin::Maintenance, move |db| {
                    db.try_with_connection(|c| {
                        probe(Checkpoint::BeforePublishReceipt)?;
                        c.execute(
                            concat!(
                                "UPDATE _day_sync_transactions SET published=1 WHERE hash=? ",
                                "AND local=1"
                            ),
                            &[text(hash)],
                        )?;
                        Ok(())
                    })
                })
                .await?;
            (self.probe)(Checkpoint::Published)?;
            report.uploaded += 1;
        }
        let names = store.list(&directory).await?;
        if names.len() > wire::MAX_OBJECTS + wire::MAX_ACTORS + 2 {
            return Err(Error::Limit);
        }
        let mut listed_hashes = std::collections::BTreeSet::new();
        let mut rejected = 0usize;
        for name in names {
            if !name.starts_with("tx-") {
                continue;
            }
            let (actor, seq, hash) = match wire::parse_filename(&name) {
                Ok(v) => v,
                Err(_) => {
                    rejected += 1;
                    continue;
                }
            };
            listed_hashes.insert(hash.clone());
            // Even already known dots must be checked against a newly listed hash (cloned actor).
            let known = self
                .db
                .read(move |db| {
                    db.try_with_connection(|c| {
                        rows(
                            c,
                            concat!(
                                "SELECT hash FROM _day_sync_transactions WHERE actor=? AND ",
                                "sequence=?"
                            ),
                            &[text(actor), int(seq)],
                        )?
                        .pop()
                        .map(|r| r[0].as_text().map(str::to_owned))
                        .transpose()
                    })
                })
                .await?;
            if known.as_deref() == Some(&hash) {
                continue;
            }
            let bytes = store
                .get(&directory, &name)
                .await?
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?;
            match self
                .receive(Object {
                    filename: name,
                    bytes,
                })
                .await
            {
                Ok(true) => report.downloaded += 1,
                Ok(false) => {}
                Err(Error::Integrity | Error::WrongSpace | Error::InvalidWire | Error::Limit) => {
                    // Malformed bytes are not a valid competing history. Never quarantine from
                    // bytes that failed structural validation, and never invent the missing predecessor.
                    rejected += 1;
                }
                Err(e) => return Err(e),
            }
        }
        // A successful empty listing after a server restore must not erase the distinction
        // between "caught up" and missing history. Never interpret absence as a user deletion.
        let missing = self
            .db
            .read(move |db| {
                db.try_with_connection(|c| {
                    Ok(rows(
                        c,
                        concat!(
                            "SELECT hash FROM _day_sync_transactions WHERE published=1 ",
                            "OR local=0"
                        ),
                        &[],
                    )?
                    .iter()
                    .any(|r| {
                        r[0].as_text()
                            .map(|h| !listed_hashes.contains(h))
                            .unwrap_or(true)
                    }))
                })
            })
            .await?;
        report.applied = self.apply_ready().await?;
        if missing {
            return Err(Error::RemoteHistoryMissing);
        }
        if rejected != 0 {
            return Err(Error::RemoteObjectsRejected { count: rejected });
        }
        report.waiting = self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    Ok((
                        number(
                            &rows(
                                c,
                                "SELECT count(*) FROM _day_sync_transactions WHERE applied=0",
                                &[],
                            )?[0][0],
                        )? as usize,
                        storage::held(c)?,
                    ))
                })
            })
            .await
            .map(|(waiting, (held, required))| {
                report.held = held as usize;
                report.required_revision = required;
                waiting
            })?;
        Ok(report)
    }
    pub async fn close(&self) -> Result<(), Error> {
        Ok(self.db.close().await?)
    }
}

#[cfg(test)]
mod validation;
