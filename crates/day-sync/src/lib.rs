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
mod compaction;
mod envelope;
mod genesis;
mod identity;
mod state;
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
/// Maintenance bounds for a single sync or compaction attempt.
#[derive(Clone, Copy, Debug)]
pub struct SyncOptions {
    /// Fully read known snapshots now. Otherwise audit every 16 attempts; missing names
    /// are detected on every listing. Unknown snapshots are always fully verified.
    pub verify_snapshots: bool,
    /// Maximum remote DELETE requests, including already absent objects.
    pub max_deletions: usize,
}
impl Default for SyncOptions {
    fn default() -> Self {
        Self {
            verify_snapshots: false,
            max_deletions: 128,
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub uploaded: usize,
    /// Complete state exceeds the snapshot budget; ordinary records remain publishable.
    pub compaction_capacity_limited: bool,
    pub snapshots_verified: usize,
    pub snapshots_published: usize,
    /// Name of the snapshot published or reverified for cleanup in this attempt.
    pub published_snapshot: Option<String>,
    pub deletion_requests: usize,
    pub objects_reclaimed: usize,
    pub pending_deletions: usize,
    /// Some listed records were left for the next attempt to keep the inbox bounded.
    pub downloads_deferred: bool,
    /// A new snapshot-sized outbox remains after this pass's publication. Retry to publish it.
    pub uploads_deferred: bool,
    /// Applied history was republished after remote loss. Unapplied history is never repaired this way.
    pub repaired_history: bool,
    pub downloaded: usize,
    pub applied: usize,
    pub waiting: usize,
    /// Objects with unreadable surviving candidates. Their display cache, if any, is local
    /// and may differ from a fresh bootstrap. Not an error: other objects keep syncing.
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
    SnapshotPrepared,
    SnapshotPartWritten,
    SnapshotManifestWritten,
    SnapshotVerified,
    SnapshotInputDeleted,
    SnapshotDeletionBatch,
    SnapshotFinalized,
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
    /// Open a v5 plaintext store. Persist public identity alongside the database. Older formats
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
                        .unwrap_or("day-sync:bookmark:5");
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
    pub async fn commit(&self, mut changes: Vec<Change>) -> Result<(), Error> {
        // Local batches may contain many bounded wire records, all committed atomically.
        // Keep a separate admission bound; the journal still enforces per-record limits.
        if changes.is_empty() || changes.len() > 65_536 {
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
        self.objects(false, None).await
    }
    async fn objects(&self, pending: bool, hash: Option<String>) -> Result<Vec<Object>, Error> {
        let identity = self.identity.clone();
        Ok(self
            .db
            .read(move |db| {
                db.try_with_connection(|c| {
                    healthy(c)?;
                    rows(
                        c,
                        if hash.is_some() {
                            "SELECT hash,bytes FROM _day_sync_transactions WHERE local=1 AND published=0 AND hash=?"
                        } else if pending {
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
                        &hash.into_iter().map(text).collect::<Vec<_>>(),
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
                    if let Some(existing) = storage::recorded_hash(c, &actor, sequence)? {
                        if existing != hash {
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
                    let heads = rows(
                        c,
                        "SELECT sequence,hash FROM _day_sync_frontiers WHERE actor=?",
                        &[text(&actor)],
                    )?;
                    if let Some(h) = heads.first()
                        && sequence <= number(&h[0])?
                    {
                        if sequence == number(&h[0])? && h[1].as_text()? != hash {
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
    pub(crate) async fn connect(
        &self,
        store: &dyn Store,
    ) -> Result<(Vec<String>, Vec<String>), Error> {
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
        let directory = vec![format!("day-sync-v5-{}", self.space)];
        store.ensure_directory(&directory).await?;
        let names = self.sync_members(store, &directory).await?;
        Ok((directory, names))
    }
    pub(crate) async fn read_records(
        &self,
        store: &dyn Store,
        directory: &[String],
        report: &mut SyncReport,
        names: Vec<String>,
        options: SyncOptions,
    ) -> Result<Option<Error>, Error> {
        if names.len() > wire::MAX_LISTING {
            return Err(Error::Limit);
        }
        // Check unknown own history and alternatives to retained record/head evidence before
        // publication or cleanup can discard that evidence. Malformed bytes never prove a fork.
        let (known, heads) = self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    let mut names = std::collections::BTreeSet::new();
                    for r in rows(
                        c,
                        "SELECT actor,sequence,hash FROM _day_sync_transactions",
                        &[],
                    )? {
                        names.insert(wire::record_name(
                            r[0].as_text()?,
                            number(&r[1])?,
                            r[2].as_text()?,
                        ));
                    }
                    for r in rows(c, "SELECT name FROM _day_sync_loaded WHERE applied=1", &[])? {
                        names.insert(r[0].as_text()?.to_owned());
                    }
                    let mut heads = std::collections::BTreeMap::new();
                    for r in rows(
                        c,
                        "SELECT actor,sequence,hash FROM _day_sync_frontiers",
                        &[],
                    )? {
                        heads.insert(
                            r[0].as_text()?.to_owned(),
                            (number(&r[1])?, r[2].as_text()?.to_owned()),
                        );
                    }
                    Ok((names, heads))
                })
            })
            .await?;
        let known_dots: std::collections::BTreeSet<_> = known
            .iter()
            .map(|name| wire::parse_filename(name).map(|(actor, sequence, _)| (actor, sequence)))
            .collect::<Result<_, _>>()?;
        let covered = |actor: &String, sequence: u64, hash: &str| {
            !known_dots.contains(&(actor.clone(), sequence))
                && heads
                    .get(actor)
                    .is_some_and(|(seq, h)| sequence < *seq || (sequence == *seq && hash == h))
        };
        for name in &names {
            if let Ok((actor, sequence, hash)) = wire::parse_filename(name)
                && (actor == self.actor
                    || known_dots.contains(&(actor.clone(), sequence))
                    || heads.get(&actor).is_some_and(|(seq, _)| sequence == *seq))
                && !known.contains(name)
                && !covered(&actor, sequence, &hash)
            {
                let bytes = store
                    .get(directory, name)
                    .await?
                    .ok_or(Error::RemoteHistoryMissing)?;
                match self
                    .receive(Object {
                        filename: name.clone(),
                        bytes,
                    })
                    .await
                {
                    Ok(_)
                    | Err(
                        Error::Integrity | Error::WrongSpace | Error::InvalidWire | Error::Limit,
                    ) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        self.reconcile_outbox(store, directory, &names, report)
            .await?;
        let (allowed, retained, pending, _) = self.compaction_status().await?;
        if allowed && (pending || retained >= wire::COMPACT_RETAINED) {
            self.automatic_compaction(store, directory, report, options)
                .await?;
        }
        let names = if allowed && (pending || retained >= wire::COMPACT_RETAINED) {
            store.list(directory).await?
        } else {
            names
        };
        if names.len() > wire::MAX_LISTING {
            return Err(Error::Limit);
        }
        let listing = names.clone();
        self.db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    if rows(c, "SELECT id FROM _day_sync_fence", &[])?.is_empty() {
                        return Ok(());
                    }
                    c.execute("DELETE FROM _day_sync_listing", &[])?;
                    for name in listing {
                        c.execute("INSERT INTO _day_sync_listing VALUES(?)", &[text(name)])?;
                    }
                    c.execute("DELETE FROM _day_sync_listing_fence", &[])?;
                    c.execute(
                        "INSERT INTO _day_sync_listing_fence SELECT 1,token FROM _day_sync_fence",
                        &[],
                    )?;
                    Ok(())
                })
            })
            .await?;
        let listed_names: std::collections::BTreeSet<_> = names.iter().cloned().collect();
        let mut rejected = 0usize;
        for name in names {
            if !name.starts_with("tx-") {
                continue;
            }
            let (actor, sequence, hash) = match wire::parse_filename(&name) {
                Ok(dot) => dot,
                Err(_) => {
                    rejected += 1;
                    continue;
                }
            };
            // Exact validated names, including individually applied cleanup receipts, are
            // immutable. An alternative hash at the same dot still goes through receive().
            if known.contains(&name) || covered(&actor, sequence, &hash) {
                // Covered raw names add neither state nor deletion authority. Retained conflicting
                // dots and head hashes must still be read and validated before any cleanup.
                continue;
            }
            let bytes = store
                .get(directory, &name)
                .await?
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?;
            match self
                .receive(Object {
                    filename: name.clone(),
                    bytes: bytes.clone(),
                })
                .await
            {
                Ok(added) => {
                    self.loaded(&Object {
                        filename: name,
                        bytes,
                    })
                    .await?;
                    if added {
                        report.downloaded += 1;
                        if report.downloaded.is_multiple_of(64) {
                            report.applied += self.apply_ready().await?;
                            let (allowed, retained, _, _) = self.compaction_status().await?;
                            if allowed && retained >= wire::COMPACT_RETAINED {
                                self.automatic_compaction(store, directory, report, options)
                                    .await?;
                                // One snapshot per pass and bounded deletion prevent unbounded
                                // maintenance. Leave the rest of the listing for a later attempt.
                                if self.compaction_status().await?.1 >= wire::COMPACT_RETAINED {
                                    report.downloads_deferred = true;
                                    break;
                                }
                            }
                        }
                    }
                }
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
                            "SELECT actor,sequence,hash FROM _day_sync_transactions WHERE published=1 ",
                            "OR local=0"
                        ),
                        &[],
                    )?
                    .iter()
                    .any(|r| {
                        match (r[0].as_text(), number(&r[1]), r[2].as_text()) {
                            (Ok(actor), Ok(sequence), Ok(hash)) => !listed_names.contains(&wire::record_name(actor, sequence, hash)),
                            _ => true,
                        }
                    }))
                })
            })
            .await?;
        report.applied += self.apply_ready().await?;
        if missing {
            return Ok(Some(Error::RemoteHistoryMissing));
        }
        if rejected != 0 {
            return Ok(Some(Error::RemoteObjectsRejected { count: rejected }));
        }
        Ok(None)
    }
    // One exact persisted object at a time keeps reconciliation memory bounded.
    async fn reconcile_outbox(
        &self,
        store: &dyn Store,
        directory: &[String],
        names: &[String],
        report: &mut SyncReport,
    ) -> Result<(), Error> {
        let listed: std::collections::BTreeSet<_> = names.iter().cloned().collect();
        let hashes = self.db.read(move |db| db.try_with_connection(|c| {
            rows(c, "SELECT actor,sequence,hash FROM _day_sync_transactions WHERE local=1 AND published=0 ORDER BY sequence", &[])?
                .into_iter().filter_map(|r| {
                    let entry = (|| {
                        let hash = r[2].as_text()?.to_owned();
                        let name = wire::record_name(r[0].as_text()?, number(&r[1])?, &hash);
                        Ok::<_, DbError>((name, hash))
                    })();
                    match entry {
                        Ok((name, hash)) => listed.contains(&name).then_some(Ok(hash)),
                        Err(e) => Some(Err(e)),
                    }
                }).collect::<Result<Vec<_>, _>>()
        })).await?;
        for hash in hashes {
            if let Some(object) = self.objects(true, Some(hash)).await?.pop() {
                self.upload(store, directory, object).await?;
                report.uploaded += 1;
            }
        }
        Ok(())
    }
    async fn upload(
        &self,
        store: &dyn Store,
        directory: &[String],
        object: Object,
    ) -> Result<(), Error> {
        // Reconcile first: a lost write acknowledgement must not force a second upload.
        let present = store.get(directory, &object.filename).await?.as_deref()
            == Some(object.bytes.as_slice());
        if !present {
            (self.probe)(Checkpoint::BeforeUpload)?;
            // Only our persisted content-addressed object. Repair partial bytes under this
            // name with exactly the same bytes; correctness does not rely on a conditional
            // write.
            store
                .put(directory, &object.filename, object.bytes.clone())
                .await?;
            (self.probe)(Checkpoint::Uploaded)?;
            if store.get(directory, &object.filename).await?.as_deref()
                != Some(object.bytes.as_slice())
            {
                return Err(Error::Integrity);
            }
        }
        (self.probe)(Checkpoint::Verified)?;
        let (_, _, hash) = wire::parse_filename(&object.filename)?;
        let probe = self.probe.clone();
        let name = object.filename.clone();
        self.db
                .write_with_origin(WriteOrigin::Maintenance, move |db| {
                    db.try_with_connection(|c| {
                        probe(Checkpoint::BeforePublishReceipt)?;
                        c.execute(
                            concat!(
                                "UPDATE _day_sync_transactions SET published=1 WHERE hash=? ",
                                "AND local=1"
                            ),
                            &[text(&hash)],
                        )?;
                        // Publication and exact deletion provenance survive the same crash.
                        c.execute("INSERT INTO _day_sync_loaded(name,hash,applied) SELECT ?,hash,applied FROM _day_sync_transactions WHERE hash=? ON CONFLICT(name) DO UPDATE SET applied=max(applied,excluded.applied)", &[text(name), text(hash)])?;
                        Ok(())
                    })
                })
                .await?;
        (self.probe)(Checkpoint::Published)?;
        Ok(())
    }
    /// Synchronize once against `store`. One active run per Replica family; cancellation
    /// releases admission. Database jobs already accepted continue atomically even if the
    /// caller drops this future.
    pub async fn sync(&self, store: &dyn Store) -> Result<SyncReport, Error> {
        self.sync_with_options(store, SyncOptions::default()).await
    }
    pub async fn sync_with_options(
        &self,
        store: &dyn Store,
        options: SyncOptions,
    ) -> Result<SyncReport, Error> {
        if self.running.swap(true, Ordering::Acquire) {
            return Err(Error::Busy);
        }
        let _guard = RunGuard(self.running.clone());
        let (directory, names) = self.connect(store).await?;
        let mut report = SyncReport::default();
        let scan = self
            .download_snapshots(
                store,
                &directory,
                &names,
                options.verify_snapshots,
                &mut report,
            )
            .await?;
        let read_error = self
            .read_records(store, &directory, &mut report, names, options)
            .await?;
        let lost = scan.finish(self).await?;
        let repair = !lost.is_empty();
        self.publish_genesis(store, &directory).await?;
        let (allowed, retained, _, reclaimable) = self.compaction_status().await?;
        if allowed && (retained >= wire::COMPACT_AFTER || reclaimable) {
            self.automatic_compaction(store, &directory, &mut report, options)
                .await?;
        }
        // A resumed journal may publish only an older prefix. Leave a large new suffix
        // durable locally for the next pass instead of filling the space with raw records.
        // With deletion deliberately disabled, an old journal may never finish. Preserve
        // ordinary outbox publication in that mode rather than defer the suffix forever.
        if allowed && options.max_deletions != 0 && report.snapshots_published != 0 {
            report.uploads_deferred = self.db.read(|db| db.try_with_connection(|c| {
                number(&rows(c, "SELECT count(*) FROM _day_sync_transactions WHERE local=1 AND published=0", &[])?[0][0])
            })).await? >= wire::COMPACT_AFTER;
        }
        while !report.uploads_deferred {
            let Some(object) = self.objects(true, None).await?.pop() else {
                break;
            };
            self.upload(store, &directory, object).await?;
            report.uploaded += 1;
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
        let history_missing = matches!(read_error, Some(Error::RemoteHistoryMissing));
        let (allowed, retained, pending, reclaimable) = self.compaction_status().await?;
        if allowed
            && (repair
                || history_missing
                || pending
                || retained >= wire::COMPACT_AFTER
                || reclaimable)
        {
            self.automatic_compaction(store, &directory, &mut report, options)
                .await?;
        }
        self.check_repair(&lost, &mut report).await?;
        report.pending_deletions = self.pending_cleanup().await?;
        // Preserve the missing-history signal for this attempt, even after republishing the
        // applied prefix. An unapplied successor can still depend on genuinely missing input.
        if let Some(error) = read_error {
            return Err(error);
        }
        Ok(report)
    }
    pub async fn close(&self) -> Result<(), Error> {
        Ok(self.db.close().await?)
    }
}

#[cfg(test)]
mod validation;

#[cfg(test)]
mod state_tests;
