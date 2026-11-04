// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A fresh epoch has one explicitly typed baseline event, never copied source actor chains.
use crate::{
    Change, Error, Replica, Space, identity,
    state::{Candidate, Head, MAX_STATE, Manifest, PART_BYTES, State},
    storage::{self, error, rows, text},
    wire,
};
use day_persistence::{Value, WriteOrigin};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Genesis {
    pub kind: String,
    pub space: Space,
    pub actor: String,
    pub revision: u32,
    pub changes: Vec<(Change, u32)>,
}
impl Genesis {
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let g: Self = identity::decode(bytes, MAX_STATE)?;
        g.space.validate()?;
        if g.kind != "day-sync-v5-genesis"
            || !wire::valid_id(&g.actor)
            || g.revision == 0
            || g.revision > wire::MAX_REVISION
        {
            return Err(Error::InvalidWire);
        }
        let mut previous = None;
        for (c, req) in &g.changes {
            if !wire::valid_object(&c.id)
                || *req == 0
                || *req > g.revision
                || c.value.as_ref().is_some_and(|v| !wire::validate_value(v))
                || previous.is_some_and(|id| id >= &c.id)
            {
                return Err(Error::InvalidWire);
            }
            previous = Some(&c.id);
        }
        Ok(g)
    }
    pub fn state(&self) -> Result<State, Error> {
        let bytes = identity::encode(self)?;
        let objects = self
            .changes
            .iter()
            .map(|(c, req)| {
                (
                    c.id.clone(),
                    vec![Candidate {
                        actor: self.actor.clone(),
                        sequence: 1,
                        lamport: 1,
                        requires: *req,
                        revision: self.revision,
                        value: c.value.clone(),
                    }],
                )
            })
            .collect();
        Ok(State {
            space: self.space.clone(),
            heads: BTreeMap::from([(
                self.actor.clone(),
                Head {
                    sequence: 1,
                    lamport: 1,
                    hash: wire::digest(&bytes),
                },
            )]),
            objects,
        })
    }
}
impl Replica {
    pub(crate) async fn seed_genesis(
        &self,
        changes: Vec<Change>,
        marker: String,
    ) -> Result<(), Error> {
        let probe = self.probe.clone();
        let identity = self.identity.clone();
        let app = self.application.clone();
        self.db
            .write_with_origin(WriteOrigin::RemoteImport, move |db| {
                let changed = db.try_with_connection(|c| {
                    storage::healthy(c)?;
                    if let Some(r) =
                        rows(c, "SELECT marker FROM _day_sync_genesis WHERE id=1", &[])?.pop()
                    {
                        return if r[0].as_text()? == marker {
                            Ok(false)
                        } else {
                            Err(error("sync.genesis_fork"))
                        };
                    }
                    if !storage::frontier(c)?.is_empty()
                        || !rows(c, "SELECT actor FROM _day_sync_transactions LIMIT 1", &[])?
                            .is_empty()
                    {
                        return Err(error("sync.bootstrap_requires_empty"));
                    }
                    if !rows(c, "SELECT id FROM _day_sync_fence", &[])?.is_empty() {
                        return Err(error("sync.epoch_frozen"));
                    }
                    probe(crate::Checkpoint::BeforeJournal)?;
                    let revision = storage::reader(app.as_deref());
                    let mut values = vec![];
                    for change in changes {
                        if let Some(app) = &app {
                            app.validate(&change)?;
                        }
                        let requires = app.as_ref().map_or(1, |a| a.requires(&change));
                        values.push((change, requires));
                    }
                    let g = Genesis {
                        kind: "day-sync-v5-genesis".into(),
                        space: identity.space.clone(),
                        actor: identity.actor.clone(),
                        revision,
                        changes: values,
                    };
                    let body = identity::encode(&g).map_err(|_| error("sync.genesis_encoding"))?;
                    let checked =
                        Genesis::decode(&body).map_err(|_| error("sync.genesis_capacity"))?;
                    let state = checked
                        .state()
                        .map_err(|_| error("sync.genesis_encoding"))?;
                    let old = crate::compaction::capture(c, &identity.space)?;
                    crate::compaction::store_state(c, &state, &old, app.as_deref())?;
                    c.execute(
                        "UPDATE _day_sync_config SET next_sequence=2 WHERE id=1",
                        &[],
                    )?;
                    c.execute(
                        "INSERT INTO _day_sync_genesis VALUES(1,?,?,0)",
                        &[text(marker), Value::Blob(body)],
                    )?;
                    probe(crate::Checkpoint::AfterJournal)?;
                    Ok(true)
                })?;
                if changed {
                    db.rescan()?;
                }
                Ok(())
            })
            .await?;
        (self.probe)(crate::Checkpoint::LocalCommitted)?;
        Ok(())
    }
    pub(crate) async fn genesis_objects(&self) -> Result<Vec<crate::Object>, Error> {
        let body = self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    rows(c, "SELECT body FROM _day_sync_genesis WHERE id=1", &[])?
                        .first()
                        .map(|r| r[0].as_blob().map(Vec::from))
                        .transpose()
                })
            })
            .await?;
        let Some(body) = body else {
            return Ok(vec![]);
        };
        let m = Manifest::from_genesis(&body)?;
        let mut objects: Vec<_> = body
            .chunks(PART_BYTES)
            .enumerate()
            .map(|(i, b)| crate::Object {
                filename: m.part_name(i),
                bytes: b.to_vec(),
            })
            .collect();
        objects.push(crate::Object {
            filename: m.name(),
            bytes: identity::encode(&m)?,
        });
        Ok(objects)
    }
    pub(crate) async fn publish_genesis(
        &self,
        store: &dyn crate::Store,
        directory: &[String],
    ) -> Result<(), Error> {
        let pending = self
            .db
            .read(|db| {
                db.try_with_connection(|c| {
                    Ok(
                        !rows(c, "SELECT id FROM _day_sync_genesis WHERE published=0", &[])?
                            .is_empty(),
                    )
                })
            })
            .await?;
        if !pending {
            return Ok(());
        }
        let objects = self.genesis_objects().await?;
        for o in &objects {
            crate::lifecycle::put_verified(store, directory, &o.filename, &o.bytes).await?;
        }
        if let Some(o) = objects.last() {
            let (m, _) = self.read_snapshot(store, directory, &o.filename).await?;
            let bytes = identity::encode(&m)?;
            self.db
                .write_with_origin(WriteOrigin::Maintenance, move |db| {
                    db.try_with_connection(|c| {
                        c.execute("UPDATE _day_sync_genesis SET published=1 WHERE id=1", &[])?;
                        c.execute(
                            "INSERT OR REPLACE INTO _day_sync_snapshots(name,manifest) VALUES(?,?)",
                            &[text(m.name()), Value::Blob(bytes)],
                        )?;
                        Ok(())
                    })
                })
                .await?;
        }
        Ok(())
    }
}
