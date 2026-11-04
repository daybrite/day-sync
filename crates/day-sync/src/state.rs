// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Complete replicated state. Records must continue through the operation merge, not this join.
use crate::{BookmarkValue, Error, Space, identity, wire};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) const MAX_STATE: usize = 64 * 1024 * 1024;
pub(crate) const PART_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_MANIFEST: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Head {
    pub sequence: u64,
    pub hash: String,
    pub lamport: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Candidate {
    pub actor: String,
    pub sequence: u64,
    pub lamport: u64,
    pub requires: u32,
    pub revision: u32,
    pub value: Option<BookmarkValue>,
}
impl Candidate {
    fn key(&self) -> (&str, u64) {
        (&self.actor, self.sequence)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct State {
    pub space: Space,
    // F is exactly heads.map(sequence); do not encode a second, inconsistent vector.
    pub heads: BTreeMap<String, Head>,
    pub objects: BTreeMap<String, Vec<Candidate>>,
}
impl State {
    pub fn sequence(&self, actor: &str) -> u64 {
        self.heads.get(actor).map_or(0, |h| h.sequence)
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.space.validate()?;
        if self.heads.len() > wire::MAX_ACTORS {
            return Err(Error::Limit);
        }
        for (actor, h) in &self.heads {
            if !wire::valid_id(actor)
                || !wire::valid_hash(&h.hash)
                || h.sequence == 0
                || h.sequence > i64::MAX as u64
                || h.lamport < h.sequence
                || h.lamport > i64::MAX as u64
            {
                return Err(Error::InvalidWire);
            }
        }
        let mut dots = BTreeMap::new();
        for (id, cs) in &self.objects {
            if !wire::valid_object(id) || cs.is_empty() {
                return Err(Error::InvalidWire);
            }
            let mut previous = None;
            let mut previous_clock = 0;
            for c in cs {
                let h = self.heads.get(&c.actor).ok_or(Error::InvalidWire)?;
                if c.sequence == 0
                    || c.sequence > h.sequence
                    || c.lamport < c.sequence
                    || c.lamport > h.lamport
                    || (c.sequence == h.sequence && c.lamport != h.lamport)
                    || (c.sequence < h.sequence && c.lamport >= h.lamport)
                    || c.requires == 0
                    || c.requires > c.revision
                    || c.revision > wire::MAX_REVISION
                    || c.value.as_ref().is_some_and(|v| !wire::validate_value(v))
                    || previous.is_some_and(|p| p >= c.key())
                    || previous
                        .is_some_and(|(actor, _)| actor == c.actor && previous_clock >= c.lamport)
                {
                    return Err(Error::InvalidWire);
                }
                let metadata = (c.lamport, c.revision);
                if dots
                    .insert(c.key(), metadata)
                    .is_some_and(|old| old != metadata)
                {
                    return Err(Error::InvalidWire);
                }
                previous = Some(c.key());
                previous_clock = c.lamport;
            }
        }
        let mut previous: Option<(&str, u64)> = None;
        for ((actor, _), (clock, _)) in dots {
            if previous.is_some_and(|(a, c)| a == actor && c >= clock) {
                return Err(Error::InvalidWire);
            }
            previous = Some((actor, clock));
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let bytes = identity::encode(self)?;
        if bytes.len() > MAX_STATE {
            return Err(Error::Limit);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let s: Self = identity::decode(bytes, MAX_STATE)?;
        s.validate()?;
        Ok(s)
    }
    pub fn covers(&self, heads: &BTreeMap<String, Head>) -> bool {
        heads.iter().all(|(a, h)| {
            self.heads.get(a).is_some_and(|own| {
                own == h || (own.sequence > h.sequence && own.lamport > h.lamport)
            })
        })
    }
    pub fn join(&self, other: &Self) -> Result<Self, Error> {
        if self.space != other.space {
            return Err(Error::WrongSpace);
        }
        if self.heads == other.heads && self.objects != other.objects {
            // Without collapse, equal histories must encode exactly the same candidate state.
            return Err(Error::Integrity);
        }
        let mut merged = self.clone();
        for (a, h) in &other.heads {
            if let Some(old) = self.heads.get(a)
                && ((old.sequence < h.sequence && old.lamport >= h.lamport)
                    || (old.sequence > h.sequence && old.lamport <= h.lamport))
            {
                return Err(Error::Fork);
            }
            match merged.heads.get(a) {
                Some(old) if old.sequence == h.sequence && old != h => return Err(Error::Fork),
                Some(old) if old.sequence >= h.sequence => {}
                _ => {
                    merged.heads.insert(a.clone(), h.clone());
                }
            }
        }
        if merged.heads.len() > wire::MAX_ACTORS {
            return Err(Error::Limit);
        }
        merged.objects.clear();
        for id in self.objects.keys().chain(other.objects.keys()) {
            if merged.objects.contains_key(id) {
                continue;
            }
            let left = self.objects.get(id).map_or(&[][..], Vec::as_slice);
            let right = other.objects.get(id).map_or(&[][..], Vec::as_slice);
            let mut candidates = BTreeMap::new();
            for (cs, opposite, frontier) in [(left, right, other), (right, left, self)] {
                for c in cs {
                    let found = opposite
                        .binary_search_by_key(&c.key(), |v| v.key())
                        .ok()
                        .map(|i| &opposite[i]);
                    if found.is_some_and(|old| old != c) {
                        return Err(Error::Fork);
                    }
                    if found.is_some() || c.sequence > frontier.sequence(&c.actor) {
                        candidates.insert(c.key(), c.clone());
                    }
                }
            }
            if candidates.is_empty() {
                // Ordinary deletion is a retained candidate, never absence. A snapshot
                // cannot forget an object this epoch has already observed, even when its
                // frontier is newer. Only a separate epoch can change that contract.
                return Err(Error::Integrity);
            }
            merged
                .objects
                .insert(id.clone(), candidates.into_values().collect());
        }
        // A complete older state cannot remove anything from a complete newer state.
        // Hash-valid snapshots can still be semantically inconsistent (e.g. omit a candidate).
        if (self.covers(&other.heads) && merged != *self)
            || (other.covers(&self.heads) && merged != *other)
        {
            return Err(Error::Integrity);
        }
        Ok(merged)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Part {
    pub hash: String,
    pub bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub kind: String,
    pub space: Space,
    pub state: String,
    pub bytes: usize,
    pub heads: BTreeMap<String, Head>,
    pub parts: Vec<Part>,
}
impl Manifest {
    pub fn covers(&self, other: &Self) -> bool {
        self.space == other.space
            && other.heads.iter().all(|(a, h)| {
                self.heads
                    .get(a)
                    .is_some_and(|v| v == h || (v.sequence > h.sequence && v.lamport > h.lamport))
            })
    }
    /// Encode a validated state and derive its manifest from those exact bytes.
    pub fn encode_state(s: &State) -> Result<(Vec<u8>, Self), Error> {
        let body = s.encode()?;
        let manifest = Self::for_state_body(s, &body);
        Ok((body, manifest))
    }
    pub fn from_body(body: &[u8]) -> Result<Self, Error> {
        let s = State::decode(body)?;
        Ok(Self::for_state_body(&s, body))
    }
    // Only called with bytes produced by State::encode or accepted by State::decode.
    fn for_state_body(s: &State, body: &[u8]) -> Self {
        Self {
            kind: "snapshot".into(),
            space: s.space.clone(),
            state: wire::digest(body),
            bytes: body.len(),
            heads: s.heads.clone(),
            parts: body
                .chunks(PART_BYTES)
                .map(|b| Part {
                    hash: wire::digest(b),
                    bytes: b.len(),
                })
                .collect(),
        }
    }
    pub fn from_genesis(body: &[u8]) -> Result<Self, Error> {
        let g = crate::genesis::Genesis::decode(body)?;
        let state = g.state()?;
        Ok(Self {
            kind: "genesis".into(),
            space: g.space,
            state: wire::digest(body),
            bytes: body.len(),
            heads: state.heads,
            parts: body
                .chunks(PART_BYTES)
                .map(|b| Part {
                    hash: wire::digest(b),
                    bytes: b.len(),
                })
                .collect(),
        })
    }
    fn prefix(&self) -> &str {
        if self.kind == "genesis" {
            "genesis"
        } else {
            "snap"
        }
    }
    pub fn name(&self) -> String {
        format!("{}-{}.ds5", self.prefix(), self.state)
    }
    pub fn part_name(&self, i: usize) -> String {
        format!(
            "{}-{}.part-{i}.{}.ds5",
            self.prefix(),
            self.state,
            self.parts[i].hash
        )
    }
    pub fn names(&self) -> Vec<String> {
        std::iter::once(self.name())
            .chain((0..self.parts.len()).map(|i| self.part_name(i)))
            .collect()
    }
    pub fn decode(name: &str, bytes: &[u8], space: &Space) -> Result<Self, Error> {
        let m: Self = identity::decode(bytes, MAX_MANIFEST)?;
        if &m.space != space {
            return Err(Error::WrongSpace);
        }
        if (m.kind != "snapshot" && m.kind != "genesis")
            || !wire::valid_hash(&m.state)
            || m.name() != name
            || m.bytes == 0
            || m.bytes > MAX_STATE
            || m.parts.len() != m.bytes.div_ceil(PART_BYTES)
            || m.heads.len() > wire::MAX_ACTORS
        {
            return Err(Error::InvalidWire);
        }
        State {
            space: m.space.clone(),
            heads: m.heads.clone(),
            objects: BTreeMap::new(),
        }
        .validate()?;
        for (i, p) in m.parts.iter().enumerate() {
            if !wire::valid_hash(&p.hash) || p.bytes != (m.bytes - i * PART_BYTES).min(PART_BYTES) {
                return Err(Error::InvalidWire);
            }
        }
        Ok(m)
    }
    pub fn verify(&self, body: &[u8]) -> Result<State, Error> {
        if body.len() != self.bytes || wire::digest(body) != self.state {
            return Err(Error::Integrity);
        }
        let s = if self.kind == "genesis" {
            crate::genesis::Genesis::decode(body)?.state()?
        } else {
            State::decode(body)?
        };
        if s.space != self.space || s.heads != self.heads {
            return Err(Error::Integrity);
        }
        for (chunk, p) in body.chunks(PART_BYTES).zip(&self.parts) {
            if wire::digest(chunk) != p.hash {
                return Err(Error::Integrity);
            }
        }
        Ok(s)
    }
}
pub(crate) fn is_manifest(name: &str) -> bool {
    name.strip_prefix("snap-")
        .or_else(|| name.strip_prefix("genesis-"))
        .and_then(|n| n.strip_suffix(".ds5"))
        .is_some_and(wire::valid_hash)
}

/// Strict path grammar for package obsolete entries; never accept arbitrary store paths.
pub(crate) fn object_name(name: &str) -> bool {
    if is_manifest(name) {
        return true;
    }
    let Some(rest) = name
        .strip_prefix("snap-")
        .or_else(|| name.strip_prefix("genesis-"))
    else {
        return false;
    };
    let Some(rest) = rest.strip_suffix(".ds5") else {
        return false;
    };
    let fields: Vec<_> = rest.split('.').collect();
    if fields.len() != 3 || !wire::valid_hash(fields[0]) || !wire::valid_hash(fields[2]) {
        return false;
    }
    let Some(index) = fields[1].strip_prefix("part-") else {
        return false;
    };
    index
        .parse::<usize>()
        .is_ok_and(|i| i < MAX_STATE.div_ceil(PART_BYTES) && i.to_string() == index)
}
