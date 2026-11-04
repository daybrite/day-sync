// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Snapshot publication is a durable SQLite state machine. No network write precedes its journal.
use crate::{
    Checkpoint, Error, Identity, Object, Replica, RunGuard, Space, Store, SyncOptions, SyncReport,
    identity,
    state::{Candidate, Head, Manifest, PART_BYTES, State},
    storage::{self, error, int, number, rows, text},
    wire,
};
use day_persistence::{DbError, SqliteConnection, Value, WriteOrigin};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::atomic::Ordering,
};

pub(crate) fn capture(c: &mut dyn SqliteConnection, space: &Space) -> Result<State, DbError> {
    let mut s = State {
        space: space.clone(),
        heads: BTreeMap::new(),
        objects: BTreeMap::new(),
    };
    for r in rows(
        c,
        "SELECT actor,sequence,hash,lamport FROM _day_sync_frontiers ORDER BY actor",
        &[],
    )? {
        s.heads.insert(
            r[0].as_text()?.into(),
            Head {
                sequence: number(&r[1])?,
                hash: r[2].as_text()?.into(),
                lamport: number(&r[3])?,
            },
        );
    }
    for r in rows(
        c,
        "SELECT object,actor,sequence,lamport,requires,revision,value FROM _day_sync_candidates ORDER BY object,actor,sequence",
        &[],
    )? {
        s.objects
            .entry(r[0].as_text()?.into())
            .or_default()
            .push(Candidate {
                actor: r[1].as_text()?.into(),
                sequence: number(&r[2])?,
                lamport: number(&r[3])?,
                requires: number(&r[4])? as u32,
                revision: number(&r[5])? as u32,
                value: if r[6] == Value::Null {
                    None
                } else {
                    Some(
                        serde_json::from_slice(r[6].as_blob()?)
                            .map_err(|_| error("sync.invalid_candidate"))?,
                    )
                },
            });
    }
    Ok(s)
}
pub(crate) fn store_state(
    c: &mut dyn SqliteConnection,
    s: &State,
    old: &State,
    app: Option<&dyn crate::Application>,
) -> Result<(), DbError> {
    for (a, h) in &s.heads {
        c.execute("INSERT INTO _day_sync_frontiers VALUES(?,?,?,?) ON CONFLICT(actor) DO UPDATE SET sequence=excluded.sequence,hash=excluded.hash,lamport=excluded.lamport",&[text(a),int(h.sequence),text(&h.hash),int(h.lamport)])?;
    }
    let ids: BTreeSet<_> = old.objects.keys().chain(s.objects.keys()).collect();
    for id in ids {
        if old.objects.get(id) == s.objects.get(id) {
            continue;
        }
        c.execute(
            "DELETE FROM _day_sync_candidates WHERE object=?",
            &[text(id)],
        )?;
        if let Some(cs) = s.objects.get(id) {
            for v in cs {
                let value = v
                    .value
                    .as_ref()
                    .map(identity::encode)
                    .transpose()
                    .map_err(|_| error("sync.encode"))?
                    .map(Value::Blob)
                    .unwrap_or(Value::Null);
                c.execute(
                    "INSERT INTO _day_sync_candidates VALUES(?,?,?,?,?,?,?)",
                    &[
                        text(id),
                        text(&v.actor),
                        int(v.sequence),
                        value,
                        int(v.lamport),
                        int(v.requires.into()),
                        int(v.revision.into()),
                    ],
                )?;
            }
        }
        storage::project_object(c, id, app)?;
    }
    let clock = s.heads.values().map(|h| h.lamport).max().unwrap_or(0);
    c.execute(
        "UPDATE _day_sync_config SET clock=max(clock,?) WHERE id=1",
        &[int(clock)],
    )?;
    Ok(())
}
fn prune(
    c: &mut dyn SqliteConnection,
    heads: &BTreeMap<String, Head>,
    outgoing: bool,
) -> Result<(), DbError> {
    for (a, h) in heads {
        c.execute(if outgoing {
            "DELETE FROM _day_sync_transactions WHERE actor=? AND sequence<=? AND applied=1"
        } else {
            "DELETE FROM _day_sync_transactions WHERE actor=? AND sequence<=? AND (local=0 OR published=1)"
        },&[text(a),int(h.sequence)])?;
    }
    // Snapshot coverage cannot promote an unapplied input into deletion authority.
    // Once its inbox row is gone there is no reason to keep that provisional entry.
    c.execute("DELETE FROM _day_sync_loaded WHERE applied=0 AND hash NOT IN (SELECT hash FROM _day_sync_transactions)", &[])?;
    Ok(())
}
fn allowed(c: &mut dyn SqliteConnection) -> Result<(), DbError> {
    storage::healthy(c)?;
    if !rows(c, "SELECT id FROM _day_sync_fence", &[])?.is_empty() {
        return Err(error("sync.epoch_frozen"));
    }
    Ok(())
}
pub(crate) struct SnapshotScan {
    lost: Vec<Manifest>,
    incomplete: Vec<(Manifest, Error)>,
}
impl SnapshotScan {
    pub async fn finish(self, replica: &Replica) -> Result<Vec<Manifest>, Error> {
        if !self.incomplete.is_empty() {
            let space = replica.identity.space.clone();
            let current = replica
                .db
                .read(move |db| db.try_with_connection(|c| capture(c, &space)))
                .await?;
            for (m, e) in self.incomplete {
                if !current.covers(&m.heads) {
                    return Err(e);
                }
            }
        }
        Ok(self.lost)
    }
}
impl Replica {
    pub(crate) async fn compaction_status(&self) -> Result<(bool, u64, bool, bool), Error> {
        Ok(self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    Ok((
                        rows(c, "SELECT id FROM _day_sync_fence", &[])?.is_empty(),
                        number(
                            &rows(
                                c,
                                "SELECT count(*) FROM _day_sync_transactions WHERE applied=1",
                                &[],
                            )?[0][0],
                        )?,
                        !rows(c, "SELECT id FROM _day_sync_compaction", &[])?.is_empty(),
                        // Imported snapshots can prune transaction rows while exact applied
                        // input receipts remain. Those inputs still need eventual cleanup.
                        !rows(
                            c,
                            "SELECT name FROM _day_sync_loaded WHERE applied=1 LIMIT 1 OFFSET ?",
                            &[int(wire::COMPACT_AFTER - 1)],
                        )?
                        .is_empty(),
                    ))
                })
            })
            .await?)
    }
    pub(crate) async fn pending_cleanup(&self) -> Result<usize, Error> {
        Ok(self.db.read(|db| db.try_with_connection(|c| {
            let Some(r) = rows(c, "SELECT c.inputs,coalesce(p.next_input,0) FROM _day_sync_compaction c LEFT JOIN _day_sync_compaction_progress p ON c.id=p.id", &[])?.pop() else { return Ok(0); };
            let inputs: Vec<String> = serde_json::from_slice(r[0].as_blob()?).map_err(|_| error("sync.snapshot_integrity"))?;
            inputs.len().checked_sub(number(&r[1])? as usize).ok_or_else(|| error("sync.snapshot_integrity"))
        })).await?)
    }
    pub(crate) async fn loaded(&self, object: &Object) -> Result<(), Error> {
        let name = object.filename.clone();
        let hash = wire::digest(&object.bytes);
        self.db.write_with_origin(WriteOrigin::Maintenance,move |db|db.try_with_connection(|c| {
            // Only an individually applied record earns deletion provenance. Snapshot coverage alone does not.
            let Some(applied)=rows(c,"SELECT applied FROM _day_sync_transactions WHERE hash=?",&[text(&hash)])?.first().map(|r|r[0].clone()) else { return Ok(()); };
            c.execute("INSERT INTO _day_sync_loaded VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET applied=max(applied,excluded.applied)",&[text(name),text(hash),applied])?; Ok(())
        })).await?;
        Ok(())
    }
    pub(crate) async fn poison(&self) -> Result<(), Error> {
        self.db
            .write_with_origin(WriteOrigin::Maintenance, |db| {
                db.try_with_connection(|c| {
                    c.execute("UPDATE _day_sync_config SET poisoned=1 WHERE id=1", &[])?;
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }
    pub(crate) async fn read_snapshot(
        &self,
        store: &dyn Store,
        directory: &[String],
        name: &str,
    ) -> Result<(Manifest, State), Error> {
        let bytes = store
            .get(directory, name)
            .await?
            .ok_or(Error::RemoteHistoryMissing)?;
        let m = Manifest::decode(name, &bytes, &self.identity.space)?;
        let mut body = Vec::with_capacity(m.bytes);
        for (i, p) in m.parts.iter().enumerate() {
            let bytes = store
                .get(directory, &m.part_name(i))
                .await?
                .ok_or(Error::RemoteHistoryMissing)?;
            if bytes.len() != p.bytes || wire::digest(&bytes) != p.hash {
                return Err(Error::Integrity);
            }
            body.extend_from_slice(&bytes);
        }
        let s = m.verify(&body)?;
        Ok((m, s))
    }
    pub(crate) async fn import_snapshot(&self, m: Manifest, incoming: State) -> Result<(), Error> {
        let identity = self.identity.clone();
        let app = self.application.clone();
        let result = self
            .db
            .write_with_origin(WriteOrigin::RemoteImport, move |db| {
                let result = db.try_with_connection(|c| {
                    storage::healthy(c)?;
                    let old = capture(c, &identity.space)?;
                    if incoming.sequence(&identity.actor) > old.sequence(&identity.actor) {
                        return Ok(false);
                    }
                    // Verify every available overlap before discarding any history or projecting values.
                    for (a, h) in &incoming.heads {
                        if storage::recorded_hash(c, a, h.sequence)?
                            .is_some_and(|hash| hash != h.hash)
                        {
                            return Ok(false);
                        }
                        if let Some(r) = rows(
                            c,
                            "SELECT bytes FROM _day_sync_transactions WHERE actor=? AND sequence=?",
                            &[text(a), int(h.sequence.saturating_add(1))],
                        )?
                        .pop()
                        {
                            let tx = identity
                                .decode_record(r[0].as_blob()?)
                                .map_err(|_| error("sync.local_integrity"))?;
                            if tx.previous.as_deref() != Some(&h.hash) {
                                return Ok(false);
                            }
                        }
                    }
                    let joined = match old.join(&incoming) {
                        Ok(s) => s,
                        Err(Error::Fork) => return Ok(false),
                        Err(_) => return Err(error("sync.invalid_snapshot")),
                    };
                    for a in incoming.heads.keys() {
                        let member = Identity {
                            space: identity.space.clone(),
                            actor: a.clone(),
                        }
                        .to_bytes()
                        .map_err(|_| error("sync.identity"))?;
                        storage::insert_member(c, &member, &identity)?;
                    }
                    store_state(c, &joined, &old, app.as_deref())?;
                    let manifest = identity::encode(&m).map_err(|_| error("sync.encode"))?;
                    c.execute(
                        "INSERT OR REPLACE INTO _day_sync_snapshots(name,manifest) VALUES(?,?)",
                        &[text(m.name()), Value::Blob(manifest)],
                    )?;
                    prune(c, &incoming.heads, false)?;
                    Ok(true)
                })?;
                if result {
                    db.rescan()?;
                }
                Ok(result)
            })
            .await?;
        if !result {
            self.poison().await?;
            return Err(Error::Fork);
        }
        Ok(())
    }
    /// Publish and read back the complete state, then reclaim individually loaded inputs.
    /// The same admission guard as sync/freeze prevents deletion crossing a lifecycle barrier.
    pub async fn compact(&self, store: &dyn Store) -> Result<SyncReport, Error> {
        self.compact_with_options(store, SyncOptions::default())
            .await
    }
    /// Force a full audit and publish changed state, with bounded, resumable reclamation.
    pub async fn compact_with_options(
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
            .download_snapshots(store, &directory, &names, true, &mut report)
            .await?;
        let read_error = self
            .read_records(store, &directory, &mut report, names, options)
            .await?;
        let lost = scan.finish(self).await?;
        self.compact_inner(store, &directory, &mut report, options)
            .await?;
        self.check_repair(&lost, &mut report).await?;
        if let Some(e) = read_error {
            return Err(e);
        }
        Ok(report)
    }
    pub(crate) async fn download_snapshots(
        &self,
        store: &dyn Store,
        directory: &[String],
        names: &[String],
        audit: bool,
        report: &mut SyncReport,
    ) -> Result<SnapshotScan, Error> {
        let space = self.identity.space.clone();
        let (known, pass, pending) = self.db.write_with_origin(WriteOrigin::Maintenance, move |db| {
            db.try_with_connection(|c| {
                c.execute("UPDATE _day_sync_maintenance SET pass=pass+1 WHERE id=1", &[])?;
                let pass = number(&rows(c, "SELECT pass FROM _day_sync_maintenance WHERE id=1", &[])?[0][0])?;
                let mut known = BTreeMap::new();
                for r in rows(c, "SELECT s.name,s.manifest,s.loaded,coalesce(v.pass,0) FROM _day_sync_snapshots s LEFT JOIN _day_sync_snapshot_checks v ON v.name=s.name", &[])? {
                    let m = Manifest::decode(r[0].as_text()?, r[1].as_blob()?, &space)
                        .map_err(|_| error("sync.snapshot_integrity"))?;
                    known.insert(m.name(), (m, number(&r[2])? != 0, number(&r[3])?));
                }
                let pending = rows(c, "SELECT body FROM _day_sync_compaction WHERE id=1", &[])?.pop()
                    .map(|r| Manifest::from_body(r[0].as_blob()?).map(|m| m.name()).map_err(|_| error("sync.snapshot_integrity")))
                    .transpose()?;
                Ok((known, pass, pending))
            })
        }).await?;
        let listed: BTreeSet<_> = names.iter().cloned().collect();
        let mut visible = BTreeMap::new();
        let mut incomplete = Vec::new();
        for name in names.iter().filter(|n| crate::state::is_manifest(n)) {
            if let Some((m, true, checked)) = known.get(name)
                && !audit
                && *checked != 0
                && pass.saturating_sub(*checked) < 16
                && m.names().iter().all(|n| listed.contains(n))
            {
                visible.insert(name.clone(), m.clone());
                continue;
            }
            match self.read_snapshot(store, directory, name).await {
                Ok((m, state)) => {
                    // Never skip validation of an unknown equal-frontier snapshot: it may be
                    // evidence of a cloned writer, even though its sequence adds nothing.
                    if !known.get(name).is_some_and(|(_, loaded, _)| *loaded) {
                        self.import_snapshot(m.clone(), state).await?;
                    }
                    let checked_name = name.clone();
                    self.db
                        .write_with_origin(WriteOrigin::Maintenance, move |db| {
                            db.try_with_connection(|c| {
                                c.execute(
                                    "INSERT OR REPLACE INTO _day_sync_snapshot_checks VALUES(?,?)",
                                    &[text(checked_name), int(pass)],
                                )?;
                                Ok(())
                            })
                        })
                        .await?;
                    report.snapshots_verified += 1;
                    visible.insert(name.clone(), m);
                }
                Err(
                    Error::RemoteHistoryMissing
                    | Error::Integrity
                    | Error::InvalidWire
                    | Error::WrongSpace
                    | Error::UnsupportedFormat
                    | Error::Limit,
                ) if known.contains_key(name) || pending.as_ref() == Some(name) => {
                    // Locally journaled bytes are repairable, but only after raw-record fork preflight.
                }
                Err(e @ (Error::RemoteHistoryMissing | Error::Integrity)) => {
                    // Read the header only to decide whether this is obsolete residue. It never
                    // grants deletion authority, membership, or permission to quarantine a writer.
                    let bytes = store
                        .get(directory, name)
                        .await?
                        .ok_or(Error::RemoteHistoryMissing)?;
                    let m = Manifest::decode(name, &bytes, &self.identity.space)?;
                    incomplete.push((m, e));
                }
                Err(e) => return Err(e), // Transport failures retain their original retry semantics.
            }
        }
        // A complete successor makes an old missing witness irrelevant. Keep the old receipt
        // until a future compaction cleans it; do not keep republishing on every idle attempt.
        let lost = known
            .iter()
            .filter(|(name, (_, loaded, _))| *loaded && !visible.contains_key(*name))
            .map(|(_, (m, _, _))| m)
            .filter(|m| !visible.values().any(|v| v.covers(m)))
            .cloned()
            .collect();
        Ok(SnapshotScan { lost, incomplete })
    }
    pub(crate) async fn check_repair(
        &self,
        lost: &[Manifest],
        report: &mut SyncReport,
    ) -> Result<(), Error> {
        if lost.is_empty() {
            return Ok(());
        }
        let Some(name) = report.published_snapshot.clone() else {
            return Err(Error::RemoteHistoryMissing);
        };
        let space = self.identity.space.clone();
        let replacement = self
            .db
            .read(move |db| {
                db.try_with_connection(|c| {
                    let r = rows(
                        c,
                        "SELECT manifest FROM _day_sync_snapshots WHERE name=?",
                        &[text(&name)],
                    )?
                    .pop()
                    .ok_or_else(|| error("sync.snapshot_integrity"))?;
                    Manifest::decode(&name, r[0].as_blob()?, &space)
                        .map_err(|_| error("sync.snapshot_integrity"))
                })
            })
            .await?;
        if !lost.iter().all(|m| replacement.covers(m)) {
            return Err(Error::RemoteHistoryMissing);
        }
        report.repaired_history = true;
        Ok(())
    }
    pub(crate) async fn automatic_compaction(
        &self,
        store: &dyn Store,
        directory: &[String],
        report: &mut SyncReport,
        options: SyncOptions,
    ) -> Result<(), Error> {
        if report.compaction_capacity_limited {
            return Ok(());
        }
        match self.compact_inner(store, directory, report, options).await {
            Err(Error::Limit) if report.compaction_capacity_limited => Ok(()),
            result => result,
        }
    }
    pub(crate) async fn compact_inner(
        &self,
        store: &dyn Store,
        directory: &[String],
        report: &mut SyncReport,
        options: SyncOptions,
    ) -> Result<(), Error> {
        // At most one publication/revalidation per pass. The shared report also bounds
        // deletion requests across download batching and the final maintenance check.
        if report.snapshots_published != 0 {
            return Ok(());
        }
        let space = self.identity.space.clone();
        let prepared = self
            .db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    allowed(c)?;
                    if let Some(r) = rows(
                        c,
                        "SELECT body,inputs FROM _day_sync_compaction WHERE id=1",
                        &[],
                    )?
                    .pop()
                    {
                        let body = r[0].as_blob()?.to_vec();
                        let manifest = Manifest::from_body(&body)
                            .map_err(|_| error("sync.snapshot_integrity"))?;
                        return Ok(Ok(Some((body, r[1].as_blob()?.to_vec(), manifest))));
                    }
                    let s = capture(c, &space)?;
                    let (body, m) = match Manifest::encode_state(&s) {
                        Ok(encoded) => encoded,
                        Err(e) => return Ok(Err(e)),
                    };
                    let mut inputs = vec![];
                    for r in rows(
                        c,
                        "SELECT name,hash FROM _day_sync_loaded WHERE applied=1 ORDER BY name",
                        &[],
                    )? {
                        let name = r[0].as_text()?;
                        let (actor, seq, hash) = wire::parse_filename(name)
                            .map_err(|_| error("sync.loaded_integrity"))?;
                        if hash != r[1].as_text()? {
                            return Err(error("sync.loaded_integrity"));
                        }
                        if seq <= s.sequence(&actor) {
                            inputs.push(name.to_owned());
                        }
                    }
                    for r in rows(
                        c,
                        "SELECT name,manifest FROM _day_sync_snapshots WHERE loaded=1 ORDER BY name",
                        &[],
                    )? {
                        let old = Manifest::decode(r[0].as_text()?, r[1].as_blob()?, &space)
                            .map_err(|_| error("sync.snapshot_integrity"))?;
                        if old.name() != m.name()
                            && m.covers(&old)
                        {
                            inputs.extend(old.names()); // Manifest first, then exclusively owned parts.
                        }
                    }
                    // A full reader-path audit in this very pass is sufficient for an unchanged
                    // state with nothing to reclaim. A cached witness or an unfinished journal
                    // never takes this shortcut; resumed deletion always revalidates below.
                    if inputs.is_empty() && !rows(c,
                        "SELECT s.name FROM _day_sync_snapshots s JOIN _day_sync_snapshot_checks v ON s.name=v.name JOIN _day_sync_maintenance p ON p.id=1 WHERE s.name=? AND s.loaded=1 AND v.pass=p.pass",
                        &[text(m.name())],
                    )?.is_empty() {
                        prune(c, &s.heads, true)?;
                        c.execute("UPDATE _day_sync_genesis SET published=1 WHERE id=1", &[])?;
                        return Ok(Ok(None));
                    }
                    let encoded = identity::encode(&inputs).map_err(|_| error("sync.encode"))?;
                    c.execute(
                        "INSERT INTO _day_sync_compaction(id,body,inputs) VALUES(1,?,?)",
                        &[Value::Blob(body.clone()), Value::Blob(encoded.clone())],
                    )?;
                    Ok(Ok(Some((body, encoded, m))))
                })
            })
            .await?;
        let (body, inputs, m) = match prepared {
            Ok(Some(prepared)) => prepared,
            Ok(None) => return Ok(()),
            Err(Error::Limit) => {
                // Only failure to encode the locally captured state is a capacity fallback.
                // Remote reads and verification errors must retain their failure semantics.
                report.compaction_capacity_limited = true;
                return Err(Error::Limit);
            }
            Err(e) => return Err(e),
        };
        if store.get(directory, "retired.json").await?.is_some() {
            return Err(Error::Retired);
        }
        (self.probe)(Checkpoint::SnapshotPrepared)?;
        for (i, chunk) in body.chunks(PART_BYTES).enumerate() {
            let name = m.part_name(i);
            let result = store.create(directory, &name, chunk.to_vec()).await;
            if store.get(directory, &name).await?.as_deref() != Some(chunk) {
                result?;
                store.put(directory, &name, chunk.to_vec()).await?;
            }
            (self.probe)(Checkpoint::SnapshotPartWritten)?;
        }
        let manifest_bytes = identity::encode(&m)?;
        let result = store
            .create(directory, &m.name(), manifest_bytes.clone())
            .await;
        if store.get(directory, &m.name()).await?.as_deref() != Some(manifest_bytes.as_slice()) {
            result?;
            store
                .put(directory, &m.name(), manifest_bytes.clone())
                .await?;
        }
        (self.probe)(Checkpoint::SnapshotManifestWritten)?;
        let (read_manifest, verified) = self.read_snapshot(store, directory, &m.name()).await?;
        // The normal reader validates the complete remote state, including its canonical
        // encoding and body hash. Matching the manifest binds it to our exact captured bytes;
        // decoding those local bytes again adds no evidence.
        if read_manifest != m {
            return Err(Error::Integrity);
        }
        let heads = verified.heads;
        let manifest = identity::encode(&m)?;
        let snapshot_name = m.name();
        let next = self.db.write_with_origin(WriteOrigin::Maintenance, move |db| {
            db.try_with_connection(|c| {
                allowed(c)?;
                c.execute("UPDATE _day_sync_compaction SET verified=1 WHERE id=1", &[])?;
                // The verified snapshot publishes its captured outbox prefix, even if cleanup
                // spans many attempts. Edits made after capture remain ordinary pending records.
                prune(c, &heads, true)?;
                c.execute("UPDATE _day_sync_genesis SET published=1 WHERE id=1", &[])?;
                c.execute("INSERT OR REPLACE INTO _day_sync_snapshots(name,manifest) VALUES(?,?)", &[text(&snapshot_name), Value::Blob(manifest)])?;
                c.execute("INSERT OR REPLACE INTO _day_sync_snapshot_checks SELECT ?,pass FROM _day_sync_maintenance WHERE id=1", &[text(snapshot_name)])?;
                c.execute("INSERT OR IGNORE INTO _day_sync_compaction_progress VALUES(1,0)", &[])?;
                number(&rows(c, "SELECT next_input FROM _day_sync_compaction_progress WHERE id=1", &[])?[0][0])
            })
        }).await? as usize;
        report.snapshots_published += 1;
        report.published_snapshot = Some(m.name());
        (self.probe)(Checkpoint::SnapshotVerified)?;
        let inputs: Vec<String> = serde_json::from_slice(&inputs).map_err(|_| Error::Integrity)?;
        if next > inputs.len() {
            return Err(Error::Integrity);
        }
        let protected: BTreeSet<_> = m.names().into_iter().collect();
        let end = next
            .saturating_add(
                options
                    .max_deletions
                    .saturating_sub(report.deletion_requests),
            )
            .min(inputs.len());
        for (batch, chunk) in inputs[next..end].chunks(32).enumerate() {
            if store.get(directory, "retired.json").await?.is_some() {
                return Err(Error::Retired);
            }
            for name in chunk {
                if protected.contains(name) {
                    return Err(Error::Integrity);
                }
                // Guard covers the awaited delete; freeze cannot return across it.
                report.deletion_requests += 1;
                report.objects_reclaimed += usize::from(store.delete(directory, name).await?);
                (self.probe)(Checkpoint::SnapshotInputDeleted)?;
            }
            let completed = chunk.to_vec();
            let cursor = next + batch * 32 + chunk.len();
            self.db
                .write_with_origin(WriteOrigin::Maintenance, move |db| {
                    db.try_with_connection(|c| {
                        allowed(c)?;
                        for name in completed {
                            c.execute("DELETE FROM _day_sync_loaded WHERE name=?", &[text(&name)])?;
                            c.execute(
                                "DELETE FROM _day_sync_snapshots WHERE name=?",
                                &[text(&name)],
                            )?;
                            c.execute(
                                "DELETE FROM _day_sync_snapshot_checks WHERE name=?",
                                &[text(name)],
                            )?;
                        }
                        c.execute(
                            "UPDATE _day_sync_compaction_progress SET next_input=? WHERE id=1",
                            &[int(cursor as u64)],
                        )?;
                        Ok(())
                    })
                })
                .await?;
            (self.probe)(Checkpoint::SnapshotDeletionBatch)?;
        }
        report.pending_deletions = inputs.len() - end;
        if end == inputs.len() {
            self.db
                .write_with_origin(WriteOrigin::Maintenance, |db| {
                    db.try_with_connection(|c| {
                        allowed(c)?;
                        c.execute("DELETE FROM _day_sync_compaction WHERE id=1", &[])?;
                        c.execute("DELETE FROM _day_sync_compaction_progress WHERE id=1", &[])?;
                        Ok(())
                    })
                })
                .await?;
            (self.probe)(Checkpoint::SnapshotFinalized)?;
        }
        Ok(())
    }
}
