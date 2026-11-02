// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Public identifiers, not credentials. The server and all authorized writers are trusted.
use crate::{Error, wire};
use serde::{Deserialize, Serialize};
/// Protocol/protection selection for a whole space. Unknown formats fail closed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Space {
    pub version: u8,
    pub encoding: String,
    pub id: String,
    pub profile: String,
}
impl Space {
    pub fn new(profile: &str) -> Result<Self, Error> {
        let s = Self {
            version: 4,
            encoding: "plain-json-v1".into(),
            id: uuid::Uuid::new_v4().to_string(),
            profile: profile.into(),
        };
        s.validate()?;
        Ok(s)
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 4 || self.encoding != "plain-json-v1" {
            return Err(Error::UnsupportedFormat);
        }
        if !wire::valid_id(&self.id) || self.profile.is_empty() || self.profile.len() > 256 {
            return Err(Error::InvalidWire);
        }
        Ok(())
    }
    pub fn space(&self) -> &str {
        &self.id
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode(self)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let s: Self = decode(bytes, 2048)?;
        s.validate()?;
        Ok(s)
    }
    /// Read a space's descriptor from `store`, refusing a retired space.
    pub async fn fetch(store: &dyn crate::Store, id: &str) -> Result<Self, Error> {
        if !wire::valid_id(id) {
            return Err(Error::InvalidWire);
        }
        let directory = [format!("day-sync-v4-{id}")];
        let bytes = store
            .get(&directory, "space.json")
            .await?
            .ok_or_else(|| crate::StoreError::new(crate::StoreErrorKind::NotFound))?;
        let s = Self::from_bytes(&bytes)?;
        if s.id != id {
            return Err(Error::Integrity);
        }
        if store.get(&directory, "retired.json").await?.is_some() {
            return Err(Error::Retired);
        }
        Ok(s)
    }
}
/// Persist alongside the database. Restoring a stale copy requires a fresh actor; no secret keys.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    pub space: Space,
    pub actor: String,
}
impl Identity {
    pub fn create_space() -> Result<Self, Error> {
        Self::create_space_for("day-sync:bookmark:4")
    }
    pub fn create_space_for(profile: &str) -> Result<Self, Error> {
        Self::join(Space::new(profile)?)
    }
    pub fn join(space: Space) -> Result<Self, Error> {
        let i = Self {
            space,
            actor: uuid::Uuid::new_v4().to_string(),
        };
        i.validate()?;
        Ok(i)
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.space.validate()?;
        if !wire::valid_id(&self.actor) {
            return Err(Error::InvalidWire);
        }
        Ok(())
    }
    pub fn space(&self) -> &str {
        &self.space.id
    }
    pub fn actor(&self) -> &str {
        &self.actor
    }
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode(self)
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let i: Self = decode(bytes, 4096)?;
        i.validate()?;
        Ok(i)
    }
    pub(crate) fn encode_record(&self, tx: &wire::Transaction) -> Result<Vec<u8>, Error> {
        if tx.actor != self.actor || tx.space != self.space.id {
            return Err(Error::InvalidWire);
        }
        crate::envelope::encode(&self.space, tx)
    }
    pub(crate) fn decode_record(&self, bytes: &[u8]) -> Result<wire::Transaction, Error> {
        crate::envelope::decode(&self.space, bytes)
    }
}
pub(crate) fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(v).map_err(|_| Error::InvalidWire)
}
pub(crate) fn decode<T: serde::de::DeserializeOwned + Serialize>(
    b: &[u8],
    max: usize,
) -> Result<T, Error> {
    if b.len() > max {
        return Err(Error::Limit);
    }
    let v: T = serde_json::from_slice(b).map_err(|_| Error::InvalidWire)?;
    if encode(&v)? != b {
        return Err(Error::InvalidWire);
    }
    Ok(v)
}
