// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Experimental fenced epoch handoff. This is deliberately a coordinated rollover, not
//! transparent online GC. Freeze is irreversible. All retained members must attest the same
//! cut; missing members require explicit retirement and their unsent edits are excluded.
//! Persist new identity BEFORE preparing/publishing; never expose a staging DB
//! until checkpoint installation succeeds. Checkpoint pins must travel independently of DAV.

use crate::store::{Store, StoreError, StoreErrorKind};
use crate::{
    Change, Error, Identity, Object, Replica, Space, identity,
    storage::{self, number, rows, text},
    wire,
};
use day_persistence::{DbError, SqliteConnection, Value, WriteOrigin};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Read, Write};
use std::sync::Arc;

const MAX_MANIFEST: usize = 2 * 1024 * 1024;
const MAX_PACKAGE: usize = 64 * 1024 * 1024;
const MAX_PACKAGE_ENCODING: usize = MAX_PACKAGE + 2 * 1024 * 1024;

const PACKAGE_MAGIC: &[u8; 8] = b"DAYPKG04";

fn write_field(w: &mut impl Write, bytes: &[u8]) -> Result<(), Error> {
    let n = u32::try_from(bytes.len()).map_err(|_| Error::Limit)?;
    w.write_all(&n.to_be_bytes())
        .and_then(|_| w.write_all(bytes))
        .map_err(|_| Error::InvalidWire)
}
fn read_number(r: &mut impl Read) -> Result<usize, Error> {
    let mut b = [0; 4];
    r.read_exact(&mut b).map_err(|_| Error::InvalidWire)?;
    Ok(u32::from_be_bytes(b) as usize)
}
fn read_field(r: &mut impl Read, maximum: usize) -> Result<Vec<u8>, Error> {
    let n = read_number(r)?;
    if n > maximum {
        return Err(Error::Limit);
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b).map_err(|_| Error::InvalidWire)?;
    Ok(b)
}
fn magic(r: &mut impl Read, expected: &[u8; 8]) -> Result<(), Error> {
    let mut b = [0; 8];
    r.read_exact(&mut b).map_err(|_| Error::InvalidWire)?;
    if &b != expected {
        return Err(Error::InvalidWire);
    }
    Ok(())
}
fn invalid() -> DbError {
    storage::error("sync.invalid_checkpoint")
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Cut {
    pub space: String,
    pub profile: String,
    pub fence: String,
    pub roster_hash: String,
    pub frontier: BTreeMap<String, u64>,
    pub state_hash: String,
    pub candidates_hash: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub actor: String,
    pub cut: Cut,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Entry {
    name: String,
    hash: String,
    bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Body {
    version: u8,
    source: Space,
    cut: Cut,
    members: Vec<Vec<u8>>,
    receipts: Vec<Receipt>,
    retired: Vec<String>,
    destination: Space,
    objects: Vec<Entry>,
    obsolete: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Manifest {
    body: Body,
}
/// Persist outside the DAV server. A stale pin cannot reveal a later hidden handoff.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CheckpointPin {
    pub source: Space,
    pub hash: String,
    pub profile: String,
}
#[derive(Clone)]
pub struct Package {
    manifest: Vec<u8>,
    objects: Vec<Object>,
}
fn changes(c: &mut dyn SqliteConnection) -> Result<Vec<Change>, DbError> {
    let mut state = BTreeMap::new();
    for r in rows(
        c,
        "SELECT DISTINCT object FROM _day_sync_candidates ORDER BY object",
        &[],
    )? {
        state.insert(r[0].as_text()?.to_owned(), None);
    }
    for r in rows(
        c,
        "SELECT id,url,title FROM sync_bookmarks ORDER BY id",
        &[],
    )? {
        state.insert(
            r[0].as_text()?.to_owned(),
            Some(crate::BookmarkValue {
                url: r[1].as_text()?.into(),
                title: r[2].as_text()?.into(),
            }),
        );
    }
    Ok(state
        .into_iter()
        .map(|(id, value)| Change { id, value })
        .collect())
}
fn state_hash(changes: &[Change]) -> Result<String, Error> {
    Ok(wire::digest(&identity::encode(&changes)?))
}
fn cut(c: &mut dyn SqliteConnection) -> Result<(Cut, Vec<Change>), DbError> {
    storage::healthy(c)?;
    let fence = rows(c, "SELECT token FROM _day_sync_fence WHERE id=1", &[])?
        .pop()
        .ok_or_else(|| storage::error("sync.freeze_required"))?[0]
        .as_text()?
        .to_owned();
    if number(
        &rows(
            c,
            "SELECT count(*) FROM _day_sync_transactions WHERE applied=0 OR (local=1 AND published=0)",
            &[],
        )?[0][0],
    )? != 0
    {
        return Err(storage::error("sync.unsettled_cut"));
    }
    // A held object's projected value is stale, so members could not attest one state.
    if storage::held(c)?.0 != 0 {
        return Err(storage::error("sync.checkpoint_requires_upgrade"));
    }
    let state = changes(c)?;
    let mut candidates = vec![];
    for r in rows(
        c,
        "SELECT object,actor,sequence,value FROM _day_sync_candidates ORDER BY object,actor,sequence",
        &[],
    )? {
        candidates.push((
            r[0].as_text()?.to_owned(),
            r[1].as_text()?.to_owned(),
            number(&r[2])?,
            if r[3] == Value::Null {
                None
            } else {
                Some(r[3].as_blob()?.to_vec())
            },
        ));
    }
    let members = storage::members(c)?;
    Ok((
        Cut {
            space: storage::config(c)?[0].as_text()?.into(),
            profile: rows(c, "SELECT name FROM _day_sync_profile WHERE id=1", &[])?[0][0]
                .as_text()?
                .into(),
            fence,
            roster_hash: wire::digest(&identity::encode(&members).map_err(|_| invalid())?),
            frontier: storage::frontier(c)?,
            state_hash: state_hash(&state).map_err(|_| invalid())?,
            candidates_hash: wire::digest(&identity::encode(&candidates).map_err(|_| invalid())?),
        },
        state,
    ))
}
fn verify_receipts(body: &Body) -> Result<(), Error> {
    body.source.validate()?;
    body.destination.validate()?;
    if body.version != 4
        || body.cut.space != body.source.id
        || body.cut.profile != body.source.profile
        || body.destination.profile != body.source.profile
        || !wire::valid_id(&body.cut.fence)
        || body.destination.id == body.source.id
        || body.members.len() > wire::MAX_ACTORS
        || body.cut.roster_hash != wire::digest(&identity::encode(&body.members)?)
    {
        return Err(Error::Integrity);
    }
    let mut members = BTreeSet::new();
    for bytes in &body.members {
        let m = Identity::from_bytes(bytes)?;
        if m.space != body.source || !members.insert(m.actor) {
            return Err(Error::Integrity);
        }
    }
    let retired = body.retired.iter().cloned().collect::<BTreeSet<_>>();
    if retired.len() != body.retired.len() || !retired.is_subset(&members) {
        return Err(Error::Integrity);
    }
    let mut seen = BTreeSet::new();
    for receipt in &body.receipts {
        if receipt.cut != body.cut
            || !seen.insert(receipt.actor.clone())
            || retired.contains(&receipt.actor)
        {
            return Err(Error::Integrity);
        }
    }
    if seen.is_empty() || seen.union(&retired).cloned().collect::<BTreeSet<_>>() != members {
        return Err(Error::Integrity);
    }
    Ok(())
}
impl Package {
    pub fn pin(&self) -> Result<CheckpointPin, Error> {
        let m: Manifest = identity::decode(&self.manifest, MAX_MANIFEST)?;
        Ok(CheckpointPin {
            source: m.body.source,
            hash: wire::digest(&self.manifest),
            profile: m.body.cut.profile,
        })
    }
    pub fn destination(&self, pin: &CheckpointPin) -> Result<Space, Error> {
        Ok(self.verify(pin)?.body.destination)
    }
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }
    pub fn byte_count(&self) -> usize {
        self.manifest.len() + self.objects.iter().map(|o| o.bytes.len()).sum::<usize>()
    }
    /// Stream a bounded, length-prefixed binary package to caller-owned staging storage.
    /// Contains public metadata and plaintext only. Use an atomic rename after successful
    /// completion; an I/O error may leave a partial destination. No JSON byte arrays.
    pub fn write_to(&self, mut writer: impl Write) -> Result<(), Error> {
        if self.byte_count() > MAX_PACKAGE
            || self.manifest.len() > MAX_MANIFEST
            || self.objects.len() > wire::MAX_OBJECTS + wire::MAX_ACTORS
        {
            return Err(Error::Limit);
        }
        writer
            .write_all(PACKAGE_MAGIC)
            .map_err(|_| Error::InvalidWire)?;
        write_field(&mut writer, &self.manifest)?;
        writer
            .write_all(&(self.objects.len() as u32).to_be_bytes())
            .map_err(|_| Error::InvalidWire)?;
        for object in &self.objects {
            if object.filename.len() > 256 || object.bytes.len() > crate::envelope::MAX_RECORD {
                return Err(Error::Limit);
            }
            write_field(&mut writer, object.filename.as_bytes())?;
            write_field(&mut writer, &object.bytes)?;
        }
        Ok(())
    }
    /// Decode incrementally with per-field and cumulative allocation bounds, then validate
    /// against an independently held pin. The resulting package remains bounded in memory.
    pub fn read_from(mut reader: impl Read, pin: &CheckpointPin) -> Result<Self, Error> {
        magic(&mut reader, PACKAGE_MAGIC)?;
        let manifest = read_field(&mut reader, MAX_MANIFEST)?;
        let count = read_number(&mut reader)?;
        if count > wire::MAX_OBJECTS + wire::MAX_ACTORS {
            return Err(Error::Limit);
        }
        let mut remaining = MAX_PACKAGE - manifest.len();
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            let filename =
                String::from_utf8(read_field(&mut reader, 256)?).map_err(|_| Error::InvalidWire)?;
            let bytes = read_field(&mut reader, remaining.min(crate::envelope::MAX_RECORD))?;
            remaining -= bytes.len();
            objects.push(Object { filename, bytes });
        }
        let mut extra = [0];
        if reader.read(&mut extra).map_err(|_| Error::InvalidWire)? != 0 {
            return Err(Error::InvalidWire);
        }
        let package = Self { manifest, objects };
        package.verify(pin)?;
        Ok(package)
    }
    pub fn export(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        self.write_to(&mut bytes)?;
        Ok(bytes)
    }
    pub fn from_bytes(bytes: &[u8], pin: &CheckpointPin) -> Result<Self, Error> {
        if bytes.len() > MAX_PACKAGE_ENCODING {
            return Err(Error::Limit);
        }
        Self::read_from(Cursor::new(bytes), pin)
    }
    fn verify(&self, pin: &CheckpointPin) -> Result<Manifest, Error> {
        if self.byte_count() > MAX_PACKAGE
            || wire::digest(&self.manifest) != pin.hash
            || !wire::valid_hash(&pin.hash)
        {
            return Err(Error::Integrity);
        }
        let m: Manifest = identity::decode(&self.manifest, MAX_MANIFEST)?;
        if m.body.source != pin.source || m.body.cut.profile != pin.profile {
            return Err(Error::Integrity);
        }
        verify_receipts(&m.body)?;
        if m.body.objects.len() != self.objects.len()
            || self.objects.len() > wire::MAX_OBJECTS + wire::MAX_ACTORS
            || m.body.obsolete.len() > wire::MAX_OBJECTS
        {
            return Err(Error::Limit);
        }
        let mut seen = BTreeSet::new();
        for (entry, object) in m.body.objects.iter().zip(&self.objects) {
            if !seen.insert(&entry.name)
                || entry.name != object.filename
                || entry.bytes != object.bytes.len()
                || entry.hash != wire::digest(&object.bytes)
            {
                return Err(Error::Integrity);
            }
            if entry.name.starts_with("tx-") {
                let (_, _, hash) = wire::parse_filename(&entry.name)?;
                if hash != entry.hash {
                    return Err(Error::Integrity);
                }
            } else {
                crate::membership::member_name(&entry.name)?;
            }
        }
        let mut obsolete = BTreeSet::new();
        for name in &m.body.obsolete {
            wire::parse_filename(name)?;
            if !obsolete.insert(name) {
                return Err(Error::InvalidWire);
            }
        }
        Ok(m)
    }
    fn base(&self, pin: &CheckpointPin) -> Result<Vec<String>, Error> {
        self.verify(pin)?;
        Ok(vec![
            format!("day-handoff-v4-{}", pin.source.space()),
            format!("cp-{}", pin.hash),
        ])
    }
    /// Objects first, immutable checksummed manifest LAST. Interrupted publication is retryable.
    /// A successful return is a verified publication, not protection against later store loss.
    pub async fn publish(&self, store: &dyn Store, pin: &CheckpointPin) -> Result<(), Error> {
        let base = self.base(pin)?;
        // Bootstrap receivers immediately treat the baseline as observed remote history.
        // Publish it in the active epoch BEFORE exposing the checkpoint manifest, so their
        // first normal sync cannot mistake a partially published baseline for server rollback.
        let active = vec![format!("day-sync-v4-{}", self.destination(pin)?.space())];
        store.ensure_directory(&active).await?;
        put_verified(
            store,
            &active,
            "space.json",
            &self.destination(pin)?.to_bytes()?,
        )
        .await?;
        for o in &self.objects {
            put_verified(store, &active, &o.filename, &o.bytes).await?;
        }
        store.ensure_directory(&base).await?;
        for o in &self.objects {
            put_verified(store, &base, &o.filename, &o.bytes).await?;
        }
        put_verified(store, &base, "manifest.json", &self.manifest).await?;
        let old = vec![format!("day-sync-v4-{}", pin.source.id)];
        store.ensure_directory(&old).await?;
        put_verified(store, &old, "retired.json", &identity::encode(pin)?).await
    }
    pub async fn fetch(store: &dyn Store, pin: &CheckpointPin) -> Result<Self, Error> {
        if !wire::valid_hash(&pin.hash) || !wire::valid_id(pin.source.space()) {
            return Err(Error::InvalidWire);
        }
        let base = vec![
            format!("day-handoff-v4-{}", pin.source.space()),
            format!("cp-{}", pin.hash),
        ];
        let manifest = get_required(store, &base, "manifest.json").await?;
        if wire::digest(&manifest) != pin.hash {
            return Err(Error::Integrity);
        }
        let m: Manifest = identity::decode(&manifest, MAX_MANIFEST)?;
        if m.body.source != pin.source || m.body.cut.profile != pin.profile {
            return Err(Error::Integrity);
        }
        verify_receipts(&m.body)?;
        if m.body.objects.len() > wire::MAX_OBJECTS + wire::MAX_ACTORS {
            return Err(Error::Limit);
        }
        let mut size = manifest.len();
        let mut objects = vec![];
        for entry in &m.body.objects {
            size = size.checked_add(entry.bytes).ok_or(Error::Limit)?;
            if size > MAX_PACKAGE {
                return Err(Error::Limit);
            }
            if entry.name.starts_with("tx-") {
                wire::parse_filename(&entry.name)?;
            } else {
                crate::membership::member_name(&entry.name)?;
            }
            let bytes = get_required(store, &base, &entry.name).await?;
            if bytes.len() != entry.bytes || wire::digest(&bytes) != entry.hash {
                return Err(Error::Integrity);
            }
            objects.push(Object {
                filename: entry.name.clone(),
                bytes,
            });
        }
        let p = Self { manifest, objects };
        p.verify(pin)?;
        Ok(p)
    }
    fn transactions(
        &self,
        pin: &CheckpointPin,
        identity: &Identity,
    ) -> Result<Vec<wire::Transaction>, Error> {
        let m = self.verify(pin)?;
        if identity.space != m.body.destination {
            return Err(Error::Integrity);
        }
        let members = self
            .objects
            .iter()
            .filter(|o| o.filename.starts_with("member-"))
            .map(|o| o.bytes.clone())
            .collect::<Vec<_>>();
        let mut actors = BTreeSet::new();
        for bytes in &members {
            let member = Identity::from_bytes(bytes)?;
            if member.space != m.body.destination || !actors.insert(member.actor) {
                return Err(Error::Integrity);
            }
        }
        let mut txs = vec![];
        for o in self
            .objects
            .iter()
            .filter(|o| o.filename.starts_with("tx-"))
        {
            let tx = identity.decode_record(&o.bytes)?;
            if tx.filename(&wire::digest(&o.bytes)) != o.filename {
                return Err(Error::Integrity);
            }
            txs.push(tx);
        }
        txs.sort_by_key(|t| t.sequence);
        if actors.len() != 1 {
            return Err(Error::InvalidWire);
        }
        let author = actors.into_iter().next().ok_or(Error::InvalidWire)?;
        let mut previous = None;
        let mut state = BTreeMap::new();
        for (i, tx) in txs.iter().enumerate() {
            let context = if i == 0 {
                BTreeMap::new()
            } else {
                BTreeMap::from([(author.clone(), i as u64)])
            };
            if tx.actor != author
                || tx.sequence != (i + 1) as u64
                || tx.lamport != tx.sequence
                || tx.context != context
                || tx.previous != previous
            {
                return Err(Error::InvalidWire);
            }
            previous = Some(
                wire::parse_filename(
                    &self
                        .objects
                        .iter()
                        .find(|o| {
                            o.filename
                                .starts_with(&format!("tx-{}.{:020}.", author, tx.sequence))
                        })
                        .ok_or(Error::InvalidWire)?
                        .filename,
                )?
                .2,
            );
            for change in &tx.changes {
                state.insert(change.id.clone(), change.value.clone());
            }
        }
        let state = state
            .into_iter()
            .map(|(id, value)| Change { id, value })
            .collect::<Vec<_>>();
        if state_hash(&state)? != m.body.cut.state_hash {
            return Err(Error::Integrity);
        }
        Ok(txs)
    }
}
impl Replica {
    /// Irreversible local fence, durable before returning. Drain old uploads and exchange all
    /// remaining records AFTER freezing, then collect matching receipts. No timeout thaw.
    pub async fn freeze(&self, token: &str) -> Result<(), Error> {
        if !wire::valid_id(token) {
            return Err(Error::InvalidWire);
        }
        let token = token.to_owned();
        self.db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    storage::healthy(c)?;
                    if let Some(row) =
                        rows(c, "SELECT token FROM _day_sync_fence WHERE id=1", &[])?.first()
                    {
                        if row[0].as_text()? != token {
                            return Err(storage::error("sync.other_fence"));
                        }
                    } else {
                        // Refuse known unsupported state sizes BEFORE making the fence
                        // irreversible. This is a record-count preflight, not a guarantee
                        // against subsequent package-byte or unsettled-peer limits.
                        if changes(c)?.len() > 65_536 {
                            return Err(storage::error("sync.checkpoint_state_capacity"));
                        }
                        c.execute("INSERT INTO _day_sync_fence VALUES(1,?)", &[text(token)])?;
                    }
                    Ok(())
                })
            })
            .await?;
        Ok(())
    }
    pub async fn attest(&self) -> Result<Receipt, Error> {
        let cut = self
            .db
            .read(|db| db.try_with_connection(|c| Ok(cut(c)?.0)))
            .await?;
        let actor = self.actor.clone();
        Ok(Receipt { actor, cut })
    }
    /// A caller coordinates a terminal handoff. `retired` is an explicit exclusion list,
    /// never inferred from timeouts. Persist the target's NEW identity first.
    pub async fn prepare_checkpoint(
        &self,
        target: &Replica,
        mut receipts: Vec<Receipt>,
        mut retired: Vec<String>,
    ) -> Result<Package, Error> {
        if self.space == target.space {
            return Err(Error::Integrity);
        }
        let (source_cut,state,members,obsolete,existing)=self.db.read(|db|db.try_with_connection(|c|{
            let (cut,state)=cut(c)?;
            let obsolete=rows(c,"SELECT actor,sequence,hash FROM _day_sync_transactions ORDER BY actor,sequence",&[])?.iter().map(|r|Ok(format!("tx-{}.{:020}.{}.ds4",r[0].as_text()?,number(&r[1])?,r[2].as_text()?))).collect::<Result<Vec<_>,DbError>>()?;
            let existing=rows(c,"SELECT manifest FROM _day_sync_handoff WHERE id=1",&[])?.first().map(|r|r[0].as_blob().map(Vec::from)).transpose()?;
            Ok((cut,state,storage::members(c)?,obsolete,existing))
        })).await?;
        if retired.contains(&self.actor) {
            return Err(Error::Integrity);
        }
        receipts.sort_by(|a, b| a.actor.cmp(&b.actor));
        retired.sort();
        let mut body = Body {
            version: 4,
            source: self.space_descriptor(),
            cut: source_cut.clone(),
            members,
            receipts,
            retired,
            destination: target.space_descriptor(),
            objects: vec![],
            obsolete,
        };
        verify_receipts(&body)?;
        let profile = target
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    Ok(
                        rows(c, "SELECT name FROM _day_sync_profile WHERE id=1", &[])?[0][0]
                            .as_text()?
                            .to_owned(),
                    )
                })
            })
            .await?;
        if profile != source_cut.profile {
            return Err(Error::Integrity);
        }
        if existing.is_none() {
            if target.snapshot().await?.transactions == 0 && !state.is_empty() {
                target.commit(state.clone()).await?;
            }
            let current = target.db.read(|db| db.try_with_connection(changes)).await?;
            if current != state {
                return Err(Error::Integrity);
            }
        }
        let mut objects = target.member_objects().await?;
        objects.extend(target.local_objects().await?);
        objects.sort_by(|a, b| a.filename.cmp(&b.filename));
        let manifest = if let Some(bytes) = existing {
            let m: Manifest = identity::decode(&bytes, MAX_MANIFEST)?;
            if m.body.destination != body.destination
                || m.body.cut != body.cut
                || m.body.retired != body.retired
            {
                return Err(Error::Fork);
            }
            objects.retain(|o| m.body.objects.iter().any(|e| e.name == o.filename));
            bytes
        } else {
            body.objects = objects
                .iter()
                .map(|o| Entry {
                    name: o.filename.clone(),
                    hash: wire::digest(&o.bytes),
                    bytes: o.bytes.len(),
                })
                .collect();
            identity::encode(&Manifest { body })?
        };
        let package = Package {
            manifest: manifest.clone(),
            objects,
        };
        let pin = package.pin()?;
        package.verify(&pin)?;
        package.transactions(&pin, &target.identity)?;
        let hash = pin.hash.clone();
        self.db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    if cut(c)?.0 != source_cut {
                        return Err(storage::error("sync.cut_changed"));
                    }
                    if let Some(row) =
                        rows(c, "SELECT hash FROM _day_sync_handoff WHERE id=1", &[])?.first()
                    {
                        if row[0].as_text()? != hash {
                            return Err(storage::error("sync.handoff_fork"));
                        }
                    } else {
                        c.execute(
                            "INSERT INTO _day_sync_handoff VALUES(1,?,?)",
                            &[text(hash), Value::Blob(manifest)],
                        )?;
                    }
                    Ok(())
                })
            })
            .await?;
        Ok(package)
    }
    /// Verify the entire bounded package before atomically importing into an EMPTY staging DB.
    /// Retrying the same pin preserves subsequent edits; a different pin is refused.
    pub async fn install_checkpoint(
        &self,
        package: &Package,
        pin: &CheckpointPin,
    ) -> Result<(), Error> {
        let manifest = package.verify(pin)?;
        let txs = package.transactions(pin, &self.identity)?;
        if let Some(app) = &self.application {
            for tx in txs.iter().filter(|tx| tx.requires <= app.revision()) {
                for change in &tx.changes {
                    app.validate(change)?;
                }
            }
        }
        let objects = package.objects.clone();
        let hash = pin.hash.clone();
        let identity = self.identity.clone();
        let app = self.application.clone();
        self.db
            .write_with_origin(WriteOrigin::RemoteImport, move |db| {
                let changed = db.try_with_connection(|c| {
                    if rows(c, "SELECT name FROM _day_sync_profile WHERE id=1", &[])?[0][0]
                        .as_text()?
                        != manifest.body.cut.profile
                    {
                        return Err(invalid());
                    }
                    if let Some(r) =
                        rows(c, "SELECT hash FROM _day_sync_bootstrap WHERE id=1", &[])?.first()
                    {
                        return if r[0].as_text()? == hash {
                            Ok(false)
                        } else {
                            Err(invalid())
                        };
                    }
                    if number(&rows(c, "SELECT count(*) FROM _day_sync_transactions", &[])?[0][0])?
                        != 0
                        || !changes(c)?.is_empty()
                    {
                        return Err(storage::error("sync.bootstrap_requires_empty"));
                    }
                    for object in &objects {
                        if object.filename.starts_with("member-") {
                            storage::insert_member(c, &object.bytes, &identity)?;
                        }
                    }
                    for tx in txs {
                        if tx.actor == identity.actor() {
                            return Err(storage::error(
                                "sync.restored_actor_requires_new_identity",
                            ));
                        }
                        let object = objects
                            .iter()
                            .find(|o| {
                                o.filename
                                    .starts_with(&format!("tx-{}.{:020}.", tx.actor, tx.sequence))
                            })
                            .ok_or_else(invalid)?;
                        storage::reserve(
                            c,
                            &tx,
                            &wire::digest(&object.bytes),
                            &object.bytes,
                            false,
                        )?;
                        if !storage::ready(c, &tx, &identity)? {
                            return Err(invalid());
                        }
                        storage::merge(c, &tx, true, app.as_deref())?;
                    }
                    if state_hash(&changes(c)?).map_err(|_| invalid())?
                        != manifest.body.cut.state_hash
                    {
                        return Err(invalid());
                    }
                    c.execute("INSERT INTO _day_sync_bootstrap VALUES(1,?)", &[text(hash)])?;
                    Ok(true)
                })?;
                if changed {
                    db.rescan()?;
                }
                Ok(())
            })
            .await?;
        Ok(())
    }
    /// Deletes ONLY old transaction paths checksummed into this terminal handoff, and only after
    /// fetching/verifying every checkpoint byte. Interrupted DELETEs are idempotent. This
    /// cannot make a malicious server retain the checkpoint after verification.
    pub async fn collect_history(
        &self,
        store: &dyn Store,
        package: &Package,
        pin: &CheckpointPin,
        backup: &VerifiedBackup,
    ) -> Result<usize, Error> {
        if backup.checkpoint_hash != pin.hash {
            return Err(Error::Integrity);
        }
        let manifest = package.verify(pin)?;
        if manifest.body.source != self.space_descriptor() {
            return Err(Error::Integrity);
        }
        let endpoint = store.endpoint();
        let hash = pin.hash.clone();
        self.db
            .read(move |db| {
                db.try_with_connection(|c| {
                    if storage::config(c)?[4].as_text()? != endpoint {
                        return Err(storage::error("sync.endpoint_changed"));
                    }
                    if rows(c, "SELECT hash FROM _day_sync_handoff WHERE id=1", &[])?
                        .first()
                        .ok_or_else(invalid)?[0]
                        .as_text()?
                        != hash
                    {
                        return Err(invalid());
                    }
                    Ok(())
                })
            })
            .await?;
        Package::fetch(store, pin).await?;
        let directory = vec![format!("day-sync-v4-{}", self.space)];
        let mut count = 0;
        for name in &manifest.body.obsolete {
            if store.delete(&directory, name).await? {
                count += 1;
            }
        }
        Ok(count)
    }
}
/// The object, or the store's not-found error when the checkpoint names something it lacks.
async fn get_required(
    store: &dyn Store,
    directory: &[String],
    name: &str,
) -> Result<Vec<u8>, Error> {
    store
        .get(directory, name)
        .await?
        .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound).into())
}
/// Publish exact bytes under `name`: skip when they are already there, otherwise write and read
/// back. A failed write whose read back shows the bytes counts as published.
async fn put_verified(
    store: &dyn Store,
    directory: &[String],
    name: &str,
    bytes: &[u8],
) -> Result<(), Error> {
    if store.get(directory, name).await?.as_deref() == Some(bytes) {
        return Ok(());
    }
    if let Err(e) = store.put(directory, name, bytes.to_vec()).await
        && store.get(directory, name).await?.as_deref() != Some(bytes)
    {
        return Err(e.into());
    }
    if store.get(directory, name).await?.as_deref() != Some(bytes) {
        return Err(Error::Integrity);
    }
    Ok(())
}

/// Readback proof for this package, not proof of off-server storage or authenticity.
pub struct VerifiedBackup {
    checkpoint_hash: String,
}
impl Package {
    pub fn verify_backup(bytes: &[u8], pin: &CheckpointPin) -> Result<VerifiedBackup, Error> {
        let package = Self::from_bytes(bytes, pin)?;
        let id = Identity::join(package.destination(pin)?)?;
        package.transactions(pin, &id)?;
        Ok(VerifiedBackup {
            checkpoint_hash: pin.hash.clone(),
        })
    }
    /// Explicitly accepts loss after this checkpoint; creates a NEW space and writer identity.
    /// The caller must persist the returned public identity before seeding a staging database.
    pub fn recover(
        &self,
        pin: &CheckpointPin,
        _policy: RecoveryPolicy,
    ) -> Result<Recovered, Error> {
        let space = self.destination(pin)?;
        self.recover_to(
            pin,
            _policy,
            Arc::new(Identity::create_space_for(&space.profile)?),
        )
    }
    /// Resume recovery with the same persisted public identity after interruption.
    pub fn recover_to(
        &self,
        pin: &CheckpointPin,
        _policy: RecoveryPolicy,
        identity: Arc<Identity>,
    ) -> Result<Recovered, Error> {
        identity.validate()?;
        let source = Identity::join(self.destination(pin)?)?;
        if identity.space.profile != source.space.profile
            || identity.space.id == source.space.id
            || identity.space.id == pin.source.id
        {
            return Err(Error::WrongSpace);
        }

        let txs = self.transactions(pin, &source)?;
        let mut state = BTreeMap::new();
        for tx in txs {
            for c in tx.changes {
                state.insert(c.id, c.value);
            }
        }
        Ok(Recovered {
            identity,
            hash: pin.hash.clone(),
            changes: state
                .into_iter()
                .map(|(id, value)| Change { id, value })
                .collect(),
        })
    }
}
#[derive(Clone, Copy)]
pub enum RecoveryPolicy {
    AcceptLossAfterCheckpoint,
}
pub struct Recovered {
    pub identity: Arc<Identity>,
    hash: String,
    changes: Vec<Change>,
}
impl Recovered {
    pub async fn seed(&self, replica: &Replica) -> Result<(), Error> {
        if *replica.identity != *self.identity {
            return Err(Error::WrongSpace);
        }
        replica
            .commit_inner(self.changes.clone(), Some(self.hash.clone()))
            .await
    }
}
