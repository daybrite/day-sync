// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! The only record encoding boundary. Plaintext today; no runtime crypto negotiation/fallback.
//! A future protected format needs a new space format, separate key provider and protocol review.
use crate::{Error, Space, identity, wire::Transaction};
use serde::{Deserialize, Serialize};
pub(crate) const MAX_RECORD: usize = crate::wire::MAX_BYTES + 2048;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    space: Space,
    transaction: Transaction,
}
pub(crate) fn encode(space: &Space, tx: &Transaction) -> Result<Vec<u8>, Error> {
    space.validate()?;
    tx.encode()?;
    if tx.space != space.id {
        return Err(Error::InvalidWire);
    }
    let b = identity::encode(&Envelope {
        space: space.clone(),
        transaction: tx.clone(),
    })?;
    if b.len() > MAX_RECORD {
        return Err(Error::Limit);
    }
    Ok(b)
}
pub(crate) fn decode(space: &Space, bytes: &[u8]) -> Result<Transaction, Error> {
    space.validate()?;
    let e: Envelope = identity::decode(bytes, MAX_RECORD)?;
    e.space.validate()?;
    if e.space != *space {
        return Err(Error::WrongSpace);
    }
    e.transaction.encode()?;
    if e.transaction.space != space.id {
        return Err(Error::WrongSpace);
    }
    Ok(e.transaction)
}
