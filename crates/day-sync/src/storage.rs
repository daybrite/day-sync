// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
use crate::{
    BookmarkValue, Change, Checkpoint, Probe,
    identity::Identity,
    wire::{self, Transaction},
};
use day_persistence::{DbError, SqliteConnection, TransactionParticipant, Value, WriteOrigin};
use std::{collections::BTreeMap, fs::File, sync::Arc};

pub(crate) fn error(s: &str) -> DbError {
    DbError::driver(s)
}
pub(crate) fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into())
}
pub(crate) fn int(n: u64) -> Value {
    Value::Int(n as i64)
}
pub(crate) fn rows(
    c: &mut dyn SqliteConnection,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Vec<Value>>, DbError> {
    let mut out = vec![];
    c.query(sql, params, &mut |r| {
        out.push((0..r.len()).map(|i| r.get(i)).collect())
    })?;
    Ok(out)
}
pub(crate) fn number(v: &Value) -> Result<u64, DbError> {
    u64::try_from(v.as_int()?).map_err(|_| error("sync.invalid_counter"))
}
pub(crate) fn config(c: &mut dyn SqliteConnection) -> Result<Vec<Value>, DbError> {
    rows(
        c,
        concat!(
            "SELECT space,actor,next_sequence,clock,endpoint,poisoned ",
            "FROM _day_sync_config WHERE id=1"
        ),
        &[],
    )?
    .pop()
    .ok_or_else(|| error("sync.missing_config"))
}
pub(crate) fn healthy(c: &mut dyn SqliteConnection) -> Result<(), DbError> {
    if number(&config(c)?[5])? != 0 {
        return Err(error("sync.fork_quarantined"));
    }
    Ok(())
}
pub(crate) fn frontier(c: &mut dyn SqliteConnection) -> Result<BTreeMap<String, u64>, DbError> {
    rows(
        c,
        concat!(
            "SELECT actor,sequence FROM _day_sync_frontiers ORDER BY ",
            "actor"
        ),
        &[],
    )?
    .into_iter()
    .map(|r| Ok((r[0].as_text()?.into(), number(&r[1])?)))
    .collect()
}
pub(crate) fn install(c: &mut dyn SqliteConnection, identity: &Identity) -> Result<(), DbError> {
    let space = identity.space();
    identity.validate().map_err(|_| error("sync.identity"))?;
    c.execute_batch("PRAGMA synchronous=FULL;")?;
    c.begin()?;
    let result = (|| {
        // This is crate-owned protocol DDL, not an application resource.
        c.execute_batch(include_str!("schema.sql"))?;
        let exists = rows(
            c,
            "SELECT space,version,actor FROM _day_sync_config WHERE id=1",
            &[],
        )?;
        if let Some(row) = exists.first() {
            if number(&row[1])? != 4 {
                return Err(error("sync.unsupported_schema"));
            }
            if row[0].as_text()? != space || row[2].as_text()? != identity.actor() {
                return Err(error("sync.wrong_space"));
            }
        } else {
            if !rows(c, "SELECT id FROM sync_bookmarks LIMIT 1", &[])?.is_empty() {
                return Err(error("sync.requires_empty_initial_store"));
            }
            c.execute(concat!(
"INSERT INTO ",
"_day_sync_config(id,version,space,actor,next_sequence,clock) VALUES(1,4,?,?,1,0)"
),&[text(space),text(identity.actor())])?;
        }
        let descriptor = identity.space.to_bytes().map_err(|_| error("sync.space"))?;
        let saved = rows(c, "SELECT descriptor FROM _day_sync_space WHERE id=1", &[])?;
        if let Some(r) = saved.first() {
            if r[0].as_blob()? != descriptor {
                return Err(error("sync.wrong_space"));
            }
        } else {
            if !exists.is_empty() {
                return Err(error("sync.missing_space"));
            }
            c.execute(
                "INSERT INTO _day_sync_space VALUES(1,?)",
                &[Value::Blob(descriptor)],
            )?;
        }
        insert_member(
            c,
            &identity.to_bytes().map_err(|_| error("sync.identity"))?,
            identity,
        )?;
        if !rows(c, "SELECT object FROM _day_sync_dirty LIMIT 1", &[])?.is_empty() {
            return Err(error("sync.orphan_dirty_state"));
        }
        c.commit()
    })();
    if result.is_err() {
        let _ = c.rollback();
    }
    result
}
pub(crate) struct Journal {
    pub application: Option<Arc<dyn crate::Application>>,
    pub probe: Probe,
    pub identity: Arc<Identity>,
    pub _lease: File,
}
impl TransactionParticipant for Journal {
    fn refresh_after_commit(&self, origin: WriteOrigin) -> bool {
        self.application.is_some() && origin == WriteOrigin::Local
    }
    fn begin(&self, c: &mut dyn SqliteConnection, origin: WriteOrigin) -> Result<(), DbError> {
        if origin == WriteOrigin::Local
            && !rows(c, "SELECT id FROM _day_sync_fence", &[])?.is_empty()
        {
            return Err(error("sync.epoch_frozen"));
        }
        if origin == WriteOrigin::RemoteImport
            && !rows(c, "SELECT id FROM _day_sync_handoff", &[])?.is_empty()
        {
            return Err(error("sync.epoch_retired"));
        }
        c.execute(
            "UPDATE _day_sync_guard SET mode=? WHERE id=1",
            &[Value::Int(match origin {
                WriteOrigin::Local => 1,
                WriteOrigin::RemoteImport => 2,
                WriteOrigin::Maintenance => 0,
            })],
        )?;
        if origin != WriteOrigin::Maintenance {
            healthy(c)?;
        }
        Ok(())
    }
    fn before_commit(
        &self,
        c: &mut dyn SqliteConnection,
        origin: WriteOrigin,
    ) -> Result<(), DbError> {
        if origin == WriteOrigin::Local {
            if let Some(app) = &self.application {
                for change in app.capture(c)? {
                    app.validate(&change)?;
                    if let Some(v) = change.value {
                        c.execute(
                            concat!(
                                concat!(
                                    "INSERT INTO sync_bookmarks(id,url,title) VALUES(?,?,?) ON ",
                                    "CONFLICT(id) DO "
                                ),
                                "UPDATE SET url=excluded.url,title=excluded.title"
                            ),
                            &[text(&change.id), text(v.url), text(v.title)],
                        )?;
                    } else {
                        c.execute("DELETE FROM sync_bookmarks WHERE id=?", &[text(&change.id)])?;
                    }
                    c.execute(
                        "INSERT OR IGNORE INTO _day_sync_dirty VALUES(?)",
                        &[text(change.id)],
                    )?;
                }
            }
            loop {
                let dirty = rows(
                    c,
                    "SELECT object FROM _day_sync_dirty ORDER BY object LIMIT 128",
                    &[],
                )?;
                if dirty.is_empty() {
                    break;
                }
                {
                    (self.probe)(Checkpoint::BeforeJournal)?;
                    let cfg = config(c)?;
                    let sequence = number(&cfg[2])?;
                    let clock = number(&cfg[3])?
                        .checked_add(1)
                        .filter(|n| *n < i64::MAX as u64)
                        .ok_or_else(|| error("sync.clock_exhausted"))?;
                    if sequence >= i64::MAX as u64 {
                        return Err(error("sync.sequence_exhausted"));
                    }
                    let actor = cfg[1].as_text()?.to_owned();
                    let context = frontier(c)?;
                    let previous = if sequence == 1 {
                        None
                    } else {
                        Some(
                            rows(
                                c,
                                concat!(
                                    "SELECT hash FROM _day_sync_transactions WHERE actor=? AND ",
                                    "sequence=?"
                                ),
                                &[text(&actor), int(sequence - 1)],
                            )?
                            .pop()
                            .ok_or_else(|| error("sync.missing_predecessor"))?[0]
                                .as_text()?
                                .into(),
                        )
                    };
                    let revision = reader(self.application.as_deref());
                    // One transaction carries one required revision, so an older build still
                    // reads the changes it understands from a commit that also holds newer ones.
                    // Changes needing another revision stay dirty for the next record.
                    let mut requires = None;
                    let mut changes = vec![];
                    let mut size = 16_384;
                    for row in dirty {
                        let id = row[0].as_text()?.to_owned();
                        let value = rows(
                            c,
                            "SELECT url,title FROM sync_bookmarks WHERE id=?",
                            &[text(&id)],
                        )?
                        .pop()
                        .map(|r| {
                            Ok::<_, DbError>(BookmarkValue {
                                url: r[0].as_text()?.into(),
                                title: r[1].as_text()?.into(),
                            })
                        })
                        .transpose()?;
                        let change = Change { id, value };
                        let needed = self
                            .application
                            .as_ref()
                            .map_or(1, |app| app.requires(&change));
                        if needed == 0 || needed > revision {
                            return Err(error("sync.invalid_requirement"));
                        }
                        if *requires.get_or_insert(needed) != needed {
                            continue;
                        }
                        let bytes = serde_json::to_vec(&change)
                            .map_err(|_| error("sync.encode"))?
                            .len();
                        if size + bytes > wire::MAX_BYTES && !changes.is_empty() {
                            break;
                        }
                        size += bytes;
                        if let Some(app) = &self.application {
                            app.validate(&change)?;
                        }
                        changes.push(change);
                    }
                    let tx = Transaction {
                        version: 4,
                        requires: requires.unwrap_or(1),
                        revision,
                        space: cfg[0].as_text()?.into(),
                        actor,
                        sequence,
                        lamport: clock,
                        previous,
                        context,
                        changes,
                    };
                    let bytes = self
                        .identity
                        .encode_record(&tx)
                        .map_err(|_| error("sync.invalid_or_oversized_transaction"))?;
                    let hash = wire::digest(&bytes);
                    reserve(c, &tx, &hash, &bytes, true)?;
                    merge(
                        c,
                        &tx,
                        self.application.is_some(),
                        self.application.as_deref(),
                    )?;
                    for change in &tx.changes {
                        c.execute(
                            "DELETE FROM _day_sync_dirty WHERE object=?",
                            &[text(&change.id)],
                        )?;
                    }
                    c.execute(
                        concat!(
                            "UPDATE _day_sync_config SET next_sequence=?,clock=? WHERE ",
                            "id=1"
                        ),
                        &[int(sequence + 1), int(clock)],
                    )?;
                    (self.probe)(Checkpoint::AfterJournal)?;
                }
            }
        }
        if origin == WriteOrigin::RemoteImport {
            (self.probe)(Checkpoint::BeforeApplyCommit)?;
        }
        c.execute("DELETE FROM _day_sync_dirty", &[])?;
        c.execute("UPDATE _day_sync_guard SET mode=0 WHERE id=1", &[])?;
        Ok(())
    }
}
pub(crate) fn reserve(
    c: &mut dyn SqliteConnection,
    tx: &Transaction,
    hash: &str,
    bytes: &[u8],
    local: bool,
) -> Result<(), DbError> {
    if number(&rows(c, "SELECT count(*) FROM _day_sync_transactions", &[])?[0][0])?
        >= wire::MAX_OBJECTS as u64
    {
        return Err(error("sync.retention_limit"));
    }
    let actors = rows(c, "SELECT DISTINCT actor FROM _day_sync_transactions", &[])?;
    if actors.len() >= wire::MAX_ACTORS
        && !actors
            .iter()
            .any(|r| r[0].as_text().ok() == Some(&tx.actor))
    {
        return Err(error("sync.actor_limit"));
    }
    c.execute(concat!(
"INSERT INTO ",
"_day_sync_transactions(actor,sequence,hash,bytes,lamport,local,applied,published) VALUES(?,?,?,?,?,?,?,0)"
),&[text(&tx.actor),int(tx.sequence),text(hash),Value::Blob(bytes.to_vec()),int(tx.lamport),Value::Int(local as i64),Value::Int(local as i64)])?;
    c.execute(
        "INSERT INTO _day_sync_revisions(actor,sequence,requires,revision) VALUES(?,?,?,?)",
        &[
            text(&tx.actor),
            int(tx.sequence),
            int(tx.requires.into()),
            int(tx.revision.into()),
        ],
    )?;

    Ok(())
}
/// The record revision this build reads: its application's, or 1 for plain bookmarks.
pub(crate) fn reader(application: Option<&dyn crate::Application>) -> u32 {
    application.map_or(1, |app| app.revision())
}
/// Apply a causally ready transaction. Preserve concurrent candidates; a concurrent deletion
/// projects as deleted, otherwise the maximum (Lamport, actor, sequence) is deterministic.
/// The transaction supersedes only observed candidates its writer's revision could read; a value
/// requiring a newer revision stays a concurrent candidate for readers that understand both.
pub(crate) fn merge(
    c: &mut dyn SqliteConnection,
    tx: &Transaction,
    project: bool,
    application: Option<&dyn crate::Application>,
) -> Result<(), DbError> {
    for change in &tx.changes {
        for (actor, seen) in &tx.context {
            c.execute(
                concat!(
                    "DELETE FROM _day_sync_candidates WHERE object=? AND actor=? AND sequence<=? ",
                    "AND coalesce((SELECT r.requires FROM _day_sync_revisions r WHERE ",
                    "r.actor=_day_sync_candidates.actor AND ",
                    "r.sequence=_day_sync_candidates.sequence),1)<=?"
                ),
                &[
                    text(&change.id),
                    text(actor),
                    int(*seen),
                    int(tx.revision.into()),
                ],
            )?;
        }
        let value = change
            .value
            .as_ref()
            .map(|v| {
                serde_json::to_vec(v)
                    .map(Value::Blob)
                    .map_err(|_| error("sync.encode"))
            })
            .transpose()?
            .unwrap_or(Value::Null);
        c.execute(
            concat!(
                "INSERT INTO ",
                "_day_sync_candidates(object,actor,sequence,value) ",
                "VALUES(?,?,?,?)"
            ),
            &[text(&change.id), text(&tx.actor), int(tx.sequence), value],
        )?;
        if project {
            project_object(c, &change.id, application)?;
        }
    }
    c.execute(
        concat!(
            concat!(
                "INSERT INTO _day_sync_frontiers(actor,sequence) VALUES(?,?) ",
                "ON CONFLICT(actor) "
            ),
            "DO UPDATE SET sequence=excluded.sequence"
        ),
        &[text(&tx.actor), int(tx.sequence)],
    )?;
    c.execute(
        concat!(
            "UPDATE _day_sync_transactions SET applied=1 WHERE actor=? ",
            "AND sequence=?"
        ),
        &[text(&tx.actor), int(tx.sequence)],
    )?;
    c.execute(
        "UPDATE _day_sync_config SET clock=max(clock,?) WHERE id=1",
        &[int(tx.lamport)],
    )?;
    Ok(())
}
/// Project an object's resolved candidate. While any candidate requires a newer revision than
/// this build reads, the object is held: it resolves only the candidates this build can read (so
/// devices at one revision agree, whatever the arrival order), or keeps its projected value when
/// it can read none. An upgrade resolves the complete set, as every newer reader does.
pub(crate) fn project_object(
    c: &mut dyn SqliteConnection,
    object: &str,
    application: Option<&dyn crate::Application>,
) -> Result<(), DbError> {
    let held = rows(
        c,
        concat!(
            "SELECT max(r.requires) FROM _day_sync_candidates c JOIN _day_sync_revisions r ",
            "ON r.actor=c.actor AND r.sequence=c.sequence WHERE c.object=? AND r.requires>?"
        ),
        &[text(object), int(reader(application).into())],
    )?
    .pop()
    .filter(|r| r[0] != Value::Null);
    if let Some(row) = held {
        c.execute(
            concat!(
                "INSERT INTO _day_sync_held(object,requires) VALUES(?,?) ",
                "ON CONFLICT(object) DO UPDATE SET requires=excluded.requires"
            ),
            &[text(object), row[0].clone()],
        )?;
    } else {
        c.execute("DELETE FROM _day_sync_held WHERE object=?", &[text(object)])?;
    }
    let candidates = rows(
        c,
        concat!(
            concat!(
                "SELECT c.value FROM _day_sync_candidates c JOIN ",
                "_day_sync_transactions t ON "
            ),
            "t.actor=c.actor AND t.sequence=c.sequence WHERE c.object=? AND ",
            "coalesce((SELECT r.requires FROM _day_sync_revisions r WHERE r.actor=c.actor AND ",
            "r.sequence=c.sequence),1)<=? ORDER BY c.value IS ",
            "NULL DESC,t.lamport DESC,c.actor DESC,c.sequence DESC"
        ),
        &[text(object), int(reader(application).into())],
    )?;
    if candidates.is_empty() {
        return Ok(());
    }
    let values = candidates
        .iter()
        .map(|r| {
            if r[0] == Value::Null {
                Ok(None)
            } else {
                serde_json::from_slice(r[0].as_blob()?)
                    .map(Some)
                    .map_err(|_| error("sync.invalid_candidate"))
            }
        })
        .collect::<Result<Vec<Option<BookmarkValue>>, DbError>>()?;
    let index = if let Some(app) = application {
        // A value released by an upgrade was stored unread; check every candidate now.
        for value in &values {
            app.validate(&Change {
                id: object.into(),
                value: value.clone(),
            })?;
        }
        app.resolve(&values)?
    } else {
        0
    };
    let winner = candidates
        .get(index)
        .ok_or_else(|| error("sync.invalid_resolution"))?;
    if winner[0] == Value::Null {
        c.execute("DELETE FROM sync_bookmarks WHERE id=?", &[text(object)])?;
    } else {
        let value: BookmarkValue = serde_json::from_slice(winner[0].as_blob()?)
            .map_err(|_| error("sync.invalid_candidate"))?;
        c.execute(
            concat!(
                concat!(
                    "INSERT INTO sync_bookmarks(id,url,title) VALUES(?,?,?) ON ",
                    "CONFLICT(id) DO "
                ),
                "UPDATE SET url=excluded.url,title=excluded.title"
            ),
            &[text(object), text(value.url), text(value.title)],
        )?;
    }
    if let Some(app) = application {
        app.project(c, object, values[index].as_ref())?;
    }
    Ok(())
}
/// Project every held object this build can now read. Returns how many it released.
pub(crate) fn release_held(
    c: &mut dyn SqliteConnection,
    application: Option<&dyn crate::Application>,
) -> Result<usize, DbError> {
    let held = rows(
        c,
        "SELECT object FROM _day_sync_held WHERE requires<=? ORDER BY object",
        &[int(reader(application).into())],
    )?;
    for row in &held {
        project_object(c, row[0].as_text()?, application)?;
    }
    Ok(held.len())
}
/// Objects held for a newer build, and the highest revision they need (0 when none).
pub(crate) fn held(c: &mut dyn SqliteConnection) -> Result<(u64, u32), DbError> {
    let row = rows(
        c,
        "SELECT count(*),coalesce(max(requires),0) FROM _day_sync_held",
        &[],
    )?
    .remove(0);
    Ok((
        number(&row[0])?,
        u32::try_from(number(&row[1])?).map_err(|_| error("sync.invalid_counter"))?,
    ))
}
pub(crate) fn ready(
    c: &mut dyn SqliteConnection,
    tx: &Transaction,
    identity: &Identity,
) -> Result<bool, DbError> {
    let current = frontier(c)?;
    if current.get(&tx.actor).copied().unwrap_or(0) + 1 != tx.sequence
        || tx
            .context
            .iter()
            .any(|(actor, seq)| current.get(actor).copied().unwrap_or(0) < *seq)
    {
        return Ok(false);
    }
    let mut clock = 0;
    for (actor, seq) in &tx.context {
        let row = rows(
            c,
            concat!(
                "SELECT lamport,hash,bytes FROM _day_sync_transactions WHERE ",
                "actor=? AND sequence=? AND applied=1"
            ),
            &[text(actor), int(*seq)],
        )?
        .pop()
        .ok_or_else(|| error("sync.missing_dependency"))?;

        clock = clock.max(number(&row[0])?);
        let dependency = identity
            .decode_record(row[2].as_blob()?)
            .map_err(|_| error("sync.dependency_integrity"))?;
        if dependency
            .context
            .iter()
            .any(|(a, n)| tx.context.get(a).copied().unwrap_or(0) < *n)
        {
            return Err(error("sync.non_closed_context"));
        }
        if actor == &tx.actor && tx.previous.as_deref() != Some(row[1].as_text()?) {
            return Err(error("sync.hash_chain_mismatch"));
        }
    }
    if clock + 1 != tx.lamport {
        return Err(error("sync.invalid_lamport"));
    }
    Ok(true)
}

pub(crate) fn members(c: &mut dyn SqliteConnection) -> Result<Vec<Vec<u8>>, DbError> {
    rows(c, "SELECT bytes FROM _day_sync_members ORDER BY actor", &[])?
        .into_iter()
        .map(|r| Ok(r[0].as_blob()?.to_vec()))
        .collect()
}
pub(crate) fn insert_member(
    c: &mut dyn SqliteConnection,
    bytes: &[u8],
    identity: &Identity,
) -> Result<(), DbError> {
    let member = Identity::from_bytes(bytes).map_err(|_| error("sync.invalid_member"))?;
    if member.space != identity.space {
        return Err(error("sync.wrong_space"));
    }
    let existing = rows(
        c,
        "SELECT bytes FROM _day_sync_members WHERE actor=?",
        &[text(&member.actor)],
    )?;
    if let Some(r) = existing.first() {
        if r[0].as_blob()? != bytes {
            return Err(error("sync.member_collision"));
        }
        return Ok(());
    }
    if members(c)?.len() >= wire::MAX_ACTORS {
        return Err(error("sync.member_limit"));
    }
    c.execute(
        "INSERT INTO _day_sync_members(actor,bytes) VALUES(?,?)",
        &[text(member.actor), Value::Blob(bytes.to_vec())],
    )?;
    Ok(())
}
