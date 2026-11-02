// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
use crate::{BookmarkValue, Change, Error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_BYTES: usize = 256 * 1024;
pub const MAX_CHANGES: usize = 128;
pub const MAX_ACTORS: usize = 32;
pub const MAX_OBJECTS: usize = 4096;
/// Highest application record revision the wire accepts.
pub const MAX_REVISION: u32 = 65_535;

/// Experimental, canonical logical transaction inside a versioned plaintext envelope.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Transaction {
    pub version: u8,
    /// Lowest application revision that can read every change here. Omitted when 1, so a
    /// first-revision record keeps the original bytes.
    #[serde(default = "first_revision", skip_serializing_if = "is_first_revision")]
    pub requires: u32,
    /// The writer's own application revision. It supersedes only the candidates it could
    /// read: an older writer never silently replaces a value it could not see.
    #[serde(default = "first_revision", skip_serializing_if = "is_first_revision")]
    pub revision: u32,
    pub space: String,
    pub actor: String,
    pub sequence: u64,
    pub lamport: u64,
    pub previous: Option<String>,
    pub context: BTreeMap<String, u64>,
    pub changes: Vec<Change>,
}
fn first_revision() -> u32 {
    1
}
fn is_first_revision(n: &u32) -> bool {
    *n == 1
}
pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub(crate) fn valid_id(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok_and(|u| !u.is_nil() && u.hyphenated().to_string() == id)
}
pub(crate) fn valid_object(id: &str) -> bool {
    valid_id(id) || valid_hash(id)
}
pub(crate) fn valid_hash(hash: &str) -> bool {
    hash.len() == 64
        && hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub(crate) fn validate_value(value: &BookmarkValue) -> bool {
    value.title.len() <= 8192 && value.url.len() <= 8192
}
impl Transaction {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidWire)?;
        self.validate()?;
        if bytes.len() > MAX_BYTES {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    fn validate(&self) -> Result<(), Error> {
        if self.version != 4
            || self.requires == 0
            || self.requires > self.revision
            || self.revision > MAX_REVISION
            || !valid_id(&self.space)
            || !valid_id(&self.actor)
            || self.sequence == 0
            || self.sequence > i64::MAX as u64
            || self.lamport == 0
            || self.lamport > i64::MAX as u64
            || self.context.len() > MAX_ACTORS
            || self.changes.is_empty()
            || self.changes.len() > MAX_CHANGES
            || self.context.get(&self.actor).copied().unwrap_or(0) != self.sequence - 1
            || self
                .context
                .iter()
                .any(|(a, s)| !valid_id(a) || *s == 0 || *s > i64::MAX as u64)
            || (self.sequence == 1) != self.previous.is_none()
            || self.previous.as_ref().is_some_and(|h| !valid_hash(h))
        {
            return Err(Error::InvalidWire);
        }
        let mut previous = None;
        let mut unique = BTreeSet::new();
        for change in &self.changes {
            if !valid_object(&change.id)
                || !unique.insert(&change.id)
                || previous.is_some_and(|p: &str| p >= change.id.as_str())
                || change.value.as_ref().is_some_and(|v| !validate_value(v))
            {
                return Err(Error::InvalidWire);
            }
            previous = Some(change.id.as_str());
        }
        Ok(())
    }
    pub fn filename(&self, hash: &str) -> String {
        format!("tx-{}.{:020}.{hash}.ds4", self.actor, self.sequence)
    }
}
pub(crate) fn parse_filename(name: &str) -> Result<(String, u64, String), Error> {
    let mut p = name
        .strip_prefix("tx-")
        .and_then(|s| s.strip_suffix(".ds4"))
        .ok_or(Error::InvalidWire)?
        .split('.');
    let actor = p.next().ok_or(Error::InvalidWire)?;
    let seq = p.next().ok_or(Error::InvalidWire)?;
    let hash = p.next().ok_or(Error::InvalidWire)?;
    let n: u64 = seq.parse().map_err(|_| Error::InvalidWire)?;
    if p.next().is_some()
        || !valid_id(actor)
        || !valid_hash(hash)
        || n == 0
        || n > i64::MAX as u64
        || format!("{n:020}") != seq
    {
        return Err(Error::InvalidWire);
    }
    Ok((actor.into(), n, hash.into()))
}
