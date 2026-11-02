// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! A public device roster for coordinated checkpoints, not an authorization mechanism.
use crate::store::{Store, StoreError, StoreErrorKind};
use crate::{Error, Identity, Object, Replica, Space, storage, wire};
use day_persistence::WriteOrigin;
pub(crate) fn member_name(name: &str) -> Result<&str, Error> {
    let actor = name
        .strip_prefix("member-")
        .and_then(|s| s.strip_suffix(".json"))
        .ok_or(Error::InvalidWire)?;
    if !wire::valid_id(actor) {
        return Err(Error::InvalidWire);
    }
    Ok(actor)
}
impl Replica {
    pub(crate) async fn member_bytes(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .db
            .read(|db| db.try_with_connection(storage::members))
            .await?)
    }
    pub async fn member_objects(&self) -> Result<Vec<Object>, Error> {
        self.member_bytes()
            .await?
            .into_iter()
            .map(|bytes| {
                let m = Identity::from_bytes(&bytes)?;
                Ok(Object {
                    filename: format!("member-{}.json", m.actor),
                    bytes,
                })
            })
            .collect()
    }
    pub async fn receive_member(&self, object: Object) -> Result<(), Error> {
        let m = Identity::from_bytes(&object.bytes)?;
        if member_name(&object.filename)? != m.actor || m.space != self.identity.space {
            return Err(Error::WrongSpace);
        }
        let identity = self.identity.clone();
        self.db
            .write_with_origin(WriteOrigin::Maintenance, move |db| {
                db.try_with_connection(|c| {
                    storage::healthy(c)?;
                    // A late join may be observed while frozen, changing the cut and invalidating old receipts.
                    // After handoff the old roster is terminal; no new member may be admitted.
                    if !storage::rows(c, "SELECT id FROM _day_sync_handoff", &[])?.is_empty() {
                        return Err(storage::error("sync.epoch_retired"));
                    }
                    storage::insert_member(c, &object.bytes, &identity)
                })
            })
            .await?;
        Ok(())
    }
    pub(crate) async fn sync_members(
        &self,
        store: &dyn Store,
        directory: &[String],
    ) -> Result<(), Error> {
        if store.get(directory, "retired.json").await?.is_some() {
            return Err(Error::Retired);
        }
        let expected = self.identity.space.to_bytes()?;
        match store.get(directory, "space.json").await? {
            Some(bytes) => {
                if Space::from_bytes(&bytes)? != self.identity.space {
                    return Err(Error::WrongSpace);
                }
            }
            None => {
                // Create-only, so two founders racing for an empty space settle on one
                // descriptor; the read back decides, not the write's outcome.
                let result = store
                    .create(directory, "space.json", expected.clone())
                    .await;
                match store.get(directory, "space.json").await? {
                    Some(bytes) if bytes == expected => {}
                    _ => {
                        result?;
                        return Err(Error::Integrity);
                    }
                }
            }
        }
        let names = store.list(directory).await?;
        if names.len() > wire::MAX_OBJECTS + wire::MAX_ACTORS + 2 {
            return Err(Error::Limit);
        }
        let known = self
            .member_objects()
            .await?
            .into_iter()
            .map(|o| o.filename)
            .collect::<std::collections::BTreeSet<_>>();
        let mut listed = std::collections::BTreeSet::new();
        for name in names {
            if !name.starts_with("member-") {
                continue;
            }
            member_name(&name)?;
            listed.insert(name.clone());
            if known.contains(&name) {
                continue;
            }
            let bytes = store
                .get(directory, &name)
                .await?
                .ok_or_else(|| StoreError::new(StoreErrorKind::NotFound))?;
            self.receive_member(Object {
                filename: name,
                bytes,
            })
            .await?;
        }
        for object in self.member_objects().await? {
            let actor = member_name(&object.filename)?.to_owned();
            let query_actor = actor.clone();
            let published = self
                .db
                .read(move |db| {
                    db.try_with_connection(|c| {
                        Ok(storage::number(
                            &storage::rows(
                                c,
                                "SELECT published FROM _day_sync_members WHERE actor=?",
                                &[storage::text(query_actor)],
                            )?[0][0],
                        )? != 0)
                    })
                })
                .await?;
            if listed.contains(&object.filename) && published {
                continue;
            }
            let present = store.get(directory, &object.filename).await?.as_deref()
                == Some(object.bytes.as_slice());
            if !present {
                let result = store
                    .put(directory, &object.filename, object.bytes.clone())
                    .await;
                match store.get(directory, &object.filename).await? {
                    Some(bytes) if bytes == object.bytes => {}
                    _ => {
                        result?;
                        return Err(Error::Integrity);
                    }
                }
            }
            self.db
                .write_with_origin(WriteOrigin::Maintenance, move |db| {
                    db.try_with_connection(|c| {
                        c.execute(
                            "UPDATE _day_sync_members SET published=1 WHERE actor=?",
                            &[storage::text(actor)],
                        )?;
                        Ok(())
                    })
                })
                .await?;
        }
        Ok(())
    }
}
