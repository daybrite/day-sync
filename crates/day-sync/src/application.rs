// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
use crate::{BookmarkValue, Change};
use day_persistence::{DbError, SqliteConnection};

/// Versioned application mapping over v5's bounded two-string record envelope. Applications
/// MUST validate their namespace, canonical payload and identity before accepting a record.
/// Capture and projection execute inside the same transaction as the model/frontier changes.
/// Implementations must suppress their capture triggers during projection, and retain user
/// state independently of disposable caches. Resolve returns an index into causally concurrent
/// candidates ordered by the engine's deterministic default priority; it must be pure.
pub trait Application: Send + Sync + 'static {
    fn profile(&self) -> &'static str;
    fn install(&self, connection: &mut dyn SqliteConnection) -> Result<(), DbError>;
    fn capture(&self, connection: &mut dyn SqliteConnection) -> Result<Vec<Change>, DbError>;
    fn validate(&self, change: &Change) -> Result<(), DbError>;
    /// The highest record revision this build reads and writes. Raise it only when records gain
    /// a form an older build cannot read; every build must still read all lower revisions.
    fn revision(&self) -> u32 {
        1
    }
    /// The lowest revision that can read `change`, from 1 to `revision()`. Keep unchanged
    /// kinds of record at their original revision so older builds go on syncing them.
    fn requires(&self, _change: &Change) -> u32 {
        1
    }
    fn resolve(&self, _values: &[Option<BookmarkValue>]) -> Result<usize, DbError> {
        Ok(0)
    }
    /// Report unreadable surviving state after projection, or `None` when the hold clears.
    /// A retained display cache is local; a fresh replica need not have the same cache.
    fn held(
        &self,
        _connection: &mut dyn SqliteConnection,
        _id: &str,
        _required_revision: Option<u32>,
    ) -> Result<(), DbError> {
        Ok(())
    }
    fn project(
        &self,
        connection: &mut dyn SqliteConnection,
        id: &str,
        value: Option<&BookmarkValue>,
    ) -> Result<(), DbError>;
}
