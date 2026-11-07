<!--
Copyright © The Daybrite Project
SPDX-License-Identifier: MPL-2.0
-->

# Persistence and synchronization integration plan

Status: revision 2, under implementation. Phases 1, 2 and the persistence half of Phase 3
are being implemented against this revision; the implementation status section at the end
records what has landed.

Date: 2026-11-06.

## Recommendation

Keep day-persistence as a relational ORM and day-sync as a separate replication engine.
Their architectural fit is good: application data and replication history commit in the
same SQLite transaction, while the application defines which logical state is shared.
Combining the packages or replacing the ORM with a generic key/value store would not
address the problems identified in this assessment.

Before 1.0, in this order: fix migration correctness, hand the ORM's own change set to the
sync participant instead of rediscovering it with triggers, replace the two-string record
with a typed one while the wire format is still free to change, eliminate unnecessary
search-index maintenance, bound the growth of synchronized state, and make cache
invalidation targeted. Use the freedom to make breaking changes for these specific
improvements. Do not undertake a wholesale schema redesign without evidence.

The record shape is the wire format. It is the one change in this plan that becomes
expensive after release, so it is ordered ahead of the performance work, not after it.

## Scope and evidence

The assessment examined day-persistence, day-sync, and the Day-News application adapter.
It included disposable SQL experiments, a standalone Rust diagnostic using the local
day-persistence checkout, and read-only measurements of a real Day-News library opened in
SQLite's immutable mode. It did not change application databases.

The revisions assessed were day-sync `e5016601`, day-persistence `12480a37` and Day-News
`08ff8816`. The Day-News checkout carried uncommitted changes to the app entry and the
refresh scheduler at the time; the other two were clean.

### Existing strengths to preserve

- The transaction participant commits application changes and their journal atomically,
  and tells the participant whether a write is local, imported or maintenance.
- The ORM updates only changed columns and uses true upserts, so statements and triggers
  see exactly what changed.
- Relationship foreign keys are indexed and declared deferred; many-to-many joins have
  composite primary keys and reverse-direction indexes.
- The ORM combines repeated edits and batches compatible updates, and native SQL execution
  uses cached prepared statements.
- Tables are strict where the engine allows, the DDL is documented, and bookkeeping is one
  table, so sync's own tables coexist with the model's.
- day-sync explicitly sets `PRAGMA synchronous=FULL` and transfers logical records, not
  copies of the live SQLite database.
- Day-News already retains synchronized choices independently of disposable article caches.

### Reproduced findings

| Experiment | Result | Interpretation |
| --- | --- | --- |
| Add a required `String` field through lightweight migration; compare with a fresh database | Upgraded table has `NOT NULL=0` and accepts NULL; fresh table has `NOT NULL=1` and rejects NULL | Equivalent model declarations can produce different database constraints. |
| Inject failure while recording a staged migration's new version | Schema alteration persists, but the stored version remains old | Migration effects and their version marker are not atomic. |
| Mark 10,000 synthetic articles read, using the generated integer-key FTS trigger pattern | Median of three runs: about 62 ms with the current trigger, 3 ms with a trigger restricted to relevant changes | Updating search indexes for read-state changes is avoidable work, about six microseconds per row; it matters for bulk operations such as mark-all-read, not for single edits. |
| Insert an article with read/star already true, using Day-News's actual capture SQL on a minimal compatible schema | Arrival is recorded, but no read/star change enters the pending table; a subsequent read-state UPDATE is captured | INSERT and UPDATE do not express initial user choices equivalently. Capture by trigger sees statements, not intent. |

The FTS experiment used in-memory SQLite 3.54.1 and synthetic content. These numbers measure
one SQL operation, not end-to-end application performance or device battery consumption.

### Measured synchronized state

A real Day-News library, read in immutable mode on 2026-11-06:

| Measure | Value |
| --- | --- |
| Synchronized records | 9,799 |
| Of those, article read flags | 9,194 |
| Articles still in the library | 6,559 |
| Disk used by sync tables and their indexes | about 11.5 MiB |
| Cost per record | about 1.2 KiB |
| Average candidate value | 223 bytes |

Each record carries one boolean or one short label. Its value is stored three times: in the
adapter's archive, in the engine's projection table, and in the candidate table. Its
64-character hexadecimal identifier is stored four times, counting the automatic indexes of
rowid tables with text keys. Read flags outlive their articles, tombstones are never
collected, and the 64 MiB snapshot budget caps a space below roughly 300,000 records. For a
feed reader this is the dominant long-term cost.

## Phase 1: migration correctness

Owner: day-persistence. Priority: required before 1.0 and before relying on further schema
changes.

### Commit migration effects and version together

`ModelContainer::run_stages` committed a stage, then recorded its version separately. A
version-write failure or interruption between those operations left the database changed
but labeled with its old version, and reopening attempted the same stage again.

The version update moves into the stage transaction, before COMMIT. Failures in the stage,
the version update, and the commit follow one rollback path. Application migrations are not
required to be idempotent to compensate.

### Require fresh and upgraded schemas to agree

`lightweight_migrate` added columns without their declared constraints and examined existing
column names without validating their types and constraints.

The lightweight path supports exactly these changes: adding a column, dropping a column, and
adding or dropping an index. An added column carries its declared `NOT NULL` with the field's
Rust default as the SQL default, and a declared unique column gets a unique index, because
`ALTER TABLE ADD COLUMN` cannot carry `UNIQUE`. Every other difference (a type, a
nullability, a key, a foreign key or its delete action) is closed by a transactional table
rebuild following SQLite's documented procedure when the declared default can fill the
column, and refused with an actionable error naming the table and column otherwise. A
fingerprint is recorded only after the file's schema has been verified to match.

Schema comparison covers column types, nullability, uniqueness, primary keys, foreign keys
and delete actions, and indexes. Rebuilds preserve application data and recreate dependent
indexes and generated triggers. Generated triggers are dropped and recreated on every open,
so updated trigger logic reaches existing databases; application-owned triggers on model
tables must be recreated by their owner after open, which the sync adapter's install step
already does.

Local schema versions stay distinct from application record revisions and the sync wire
version. A local layout optimization need not change the wire format. A change to synchronized
identity or payload semantics requires a profile or revision migration.

Acceptance criteria:

- Fresh creation and every supported upgrade produce equivalent enforced constraints.
- Failure injection at each migration boundary leaves either the old state and version or
  the new state and version, never a mixture.
- A failed migration can be reopened safely without losing data.
- Incompatible schema changes are rejected rather than silently accepted.
- Upgrades pass both SQLite integrity and foreign-key checks.

Source: [schema and migration implementation](../../day-persistence/crates/day-persistence/src/lib.rs),
especially `run_stages`, `ensure_table`, and `lightweight_migrate`.

## Phase 2: change-set capture and the record shape

Owner: day-persistence for the participant contract, day-sync for the record, Day-News for
adoption. Priority: before 1.0, ahead of the performance phases, because both are easier to
build on the new contract than to redo afterwards.

### Hand the change set to the participant

The ORM knows every inserted, updated and deleted row and every changed column at save
time, and each change carries an author tag. The sync adapter discarded that knowledge and
rediscovered it with about twenty triggers and five shadow tables, which is where the
insert-versus-update capture gap comes from.

The transaction participant's `before_commit` receives the transaction's folded change set:
one entry per row with its table, key, kind (inserted, updated with the changed columns,
deleted, or rewritten by a wholesale resync) and the author tags of the changes folded into
it. The set accumulates across intermediate saves within one transaction and is cleared on
rollback. It covers every row that went through the ORM's pipeline, including relation
upkeep and in-process cascades. Rows the engine's own `ON DELETE` clause removed without the
process ever faulting them are not in the set; a participant that needs complete capture of
raw SQL keeps guard triggers for that path.

Deletes carry the key only. An adapter whose synchronized identity is a natural key held in
a non-key column must either keep its own archive of that mapping or request the removed
row; adding the removed row's values to the delete entry is a follow-up once an adapter
needs it.

### Replace the boolean refresh with a scope

`refresh_after_commit` returned a boolean, and a true answer reread every resident row,
dropped every relation memo and reran every live query after every local commit. The
participant now returns a refresh scope from `before_commit`: nothing, a set of tables, or
everything. A table-scoped refresh rereads only that table's resident rows and reruns only
the live queries over it. The distinction between a rolled-back write and a committed write
whose refresh failed is preserved.

### Replace the two-string record

The wire record is an identifier plus two strings of at most 8 KiB each. It has no model
name, no typed values, no field-level merging, and forces every typed column through a JSON
string. Day-News splits one feed row into four records to get per-field merging.

The replacement record names the model, the key, the field group and a typed value: an
identifier of 16 or 32 bytes carried in binary, a model and group name, and a value encoded
as the ORM's own column values. The engine's projection table is dropped whenever an
application adapter is installed, since the adapter's projection and the candidate table
already hold the value. Candidate, dirty and held tables are created without a rowid, which
removes their duplicate key indexes.

This is a wire version change and touches record encoding, candidates, snapshots, genesis and
compaction. It is specified here and scheduled after the persistence contract lands; it is
not part of the first implementation pass.

Acceptance criteria:

- A participant test sees inserts, column-level updates, deletes and rewrites, with author
  tags, across intermediate saves, and sees nothing after a rollback.
- A table-scoped refresh leaves other tables' resident rows and live queries untouched.
- day-sync builds against the new contract and its suite passes unchanged in behavior.
- The Day-News adapter builds against the new contract; its trigger-based capture is replaced
  in a separate step once deletes can carry natural keys.

Sources: `fold`, `flush`, `participant_commit` and `refresh_committed` in
[persistence](../../day-persistence/crates/day-persistence/src/lib.rs),
[journal](../crates/day-sync/src/storage.rs), and
[application contract](../crates/day-sync/src/application.rs).

## Phase 3: eliminate unnecessary database writes

Owner: day-persistence for generated triggers; Day-News for application projection SQL.
Priority: high for bulk operations.

Generated FTS update triggers ran on every row update. Marking an article read or starred
therefore removed and reinserted its searchable content. Trigger execution is restricted to
changes in indexed content or identity, with a value comparison so an UPDATE that assigns an
unchanged value does nothing, on both the integer-key and wide-key implementations.

Audit application projection statements for unchanged-value updates as well. Preserve
ordinary same-value assignment as a no-op at the application level. Keep an explicit
conflict-resolution action distinct: deliberately superseding concurrent alternatives can
require a journal record even when the displayed value does not change.

Acceptance criteria:

- Read/star-only changes cause no FTS index maintenance.
- Changed searchable content remains searchable after update, reopen, and rollback.
- Identity handling remains correct wherever identity updates are supported.
- Unchanged assignments produce no unintended application sync records.
- Benchmarks report SQL work and WAL growth as well as elapsed time.

Sources: `create_shadow_tables` in the
[ORM implementation](../../day-persistence/crates/day-persistence/src/lib.rs), and
[Day-News projection](../../Day-News/crates/daynews-db/src/webdav.rs).

## Phase 4: capture, durable application state and growth

Owner: Day-News, with reusable contracts in day-sync. Priority: correctness first; schema
simplification subject to review.

Capture must distinguish downloaded defaults from user intent. With the change set of Phase 2
the distinction is the author tag: downloads are written under their own author, user edits
under the default or the undo author. Specify and test downloaded insertion, user-created
initial state, initial seeding, ordinary editing, remote projection, and maintenance
separately. Do not overload the transaction's replication origin to silently suppress
unrelated user edits in a mixed operation.

Promote durable choices from adapter-specific auxiliary state into explicit application
models:

| State | Ownership and lifetime |
| --- | --- |
| Article metadata and bodies | Replaceable downloaded cache |
| Read/star choices | Durable synchronized user state |
| Subscriptions, collections, membership | Durable synchronized user state |
| Polling times, HTTP validators, download failures | Local operational state |
| Candidates, causal history, publication receipts | Sync-engine state |

A durable article-state row must survive the absence or eviction of its article cache row.
Do not attach it to that cache with a required foreign key or cascading deletion. Retain the
ability to receive a choice before downloading the corresponding article. If measurements
justify mirrored read/star columns on articles, define them as derived values rebuilt from
durable state, never as a second authority.

Bound growth. Read state is 94 percent of the measured records and grows with every article
ever read. Choose one of: an expiry for read records older than the retention window, carried
as a tombstone the way deletions are; or one compact record per feed holding the read set,
within the record size bound. Tombstone collection in the engine needs a protocol decision
and is tracked separately.

Acceptance criteria:

- Insert-and-edit within one transaction preserves intended user choices.
- Downloaded defaults do not overwrite retained choices or generate spurious sync traffic.
- Choices survive article eviction, redownload, unsubscribe, and application restart.
- Remote projection does not echo into a new local record.
- Rollback removes both application effects and pending capture state.
- The number of synchronized records is bounded by the retention window or the feed count,
  not by lifetime reading.

Sources: [capture triggers](../../Day-News/crates/daynews-db/src/webdav.sql),
[adapter](../../Day-News/crates/daynews-db/src/webdav.rs), and
[application contract](../crates/day-sync/src/application.rs).

## Phase 5: relationship, identity and ordering semantics

Owner: application schemas, supported by persistence and sync contracts. Priority: decide
before 1.0; migrate deliberately.

Retain normalized foreign keys and indexed membership tables. Do not automatically make an
entire ORM row the unit of conflict resolution: independently editable attributes and membership
often need separate logical records, which the field group in the Phase 2 record provides.

Recommended application changes:

- Use immutable IDs for user-created objects; UUIDs are already supported. Display names
  are mutable attributes. Keep natural identities where appropriate, such as a feed's
  canonical identity, with explicit handling of identity changes and collisions.
- Remove uniqueness constraints on display names unless the product requires uniqueness.
  Day-News's unique collection names currently require deterministic disambiguation and
  temporary renames during projection.
- Change organizational relationships to express their real ownership. Deleting a Day-News
  collection should detach feeds, so `SET NULL` is the declaration, instead of declaring
  cascade and preventing it by hand in the delete path. Preserve cascade for genuinely owned
  disposable data.
- Ordering: positions become text fractional keys with the writing device's actor as the
  tie-break suffix, and are never rebalanced. Two devices inserting into the same gap then
  produce distinct, deterministically ordered keys, and a move writes one row on every
  replica. Float positions with rebalancing rewrite every sibling and tie on concurrent
  inserts.

The protocol's transaction boundary needs an explicit integration contract. A local
SQLite transaction can generate multiple wire records because of the 128-change limit,
payload bounds, or application revisions. Receivers apply records separately. Local atomicity
therefore does not imply atomic visibility of the entire operation on another device.

Deferred foreign keys help within one transaction; they do not resolve references whose
targets arrive in a later transaction. Preserve unresolved logical references and project them
when their targets become available. Do not disable foreign-key enforcement to accept them.

Where an application truly requires a multi-object operation to appear atomically remotely,
first consider a single bounded logical record. Add protocol-level grouping only if a concrete
requirement remains.

Acceptance criteria include child-before-parent arrival, concurrent rename/move/delete,
delete-and-recreate, duplicate display names, concurrent inserts into one gap, and
relationship operations crossing a wire-record boundary. Membership arrival and object
deletion must converge without unexpected loss of unrelated content.

Sources: [ORM relationships](../../day-persistence/crates/day-persistence/src/relations.rs),
[Day-News models](../../Day-News/crates/daynews-db/src/lib.rs),
[collection projection](../../Day-News/crates/daynews-db/src/webdav_layout.rs), and
[journal batching](../crates/day-sync/src/storage.rs).

## Phase 6: batch projection

Owner: day-sync, adopted by Day-News. Priority: measure after the earlier phases.

Consider a batch projection interface that resolves and retains a batch's chosen state before
reconciling affected application relationships once. Day-News currently reconstructs collection
state and revisits memberships for individual collection records, potentially repeating work.
Batching must preserve candidate-resolution semantics, held newer-revision records, rollback,
and capture suppression. It does not itself make multiple wire records remotely atomic.

Acceptance criteria:

- Single-record and batch projection converge to equivalent application state.
- Projection and refresh failures preserve the existing commit/rollback guarantees.
- SQL-count and latency measurements demonstrate gains at realistic cache and library sizes.

## SQLite policy

Keep the live database on device-local storage. A shared folder contains sync-protocol files,
not a concurrently opened SQLite database.

| Area | Recommendation |
| --- | --- |
| WAL | Keep for device-local file databases. |
| Durability | Keep `synchronous=FULL`. On Apple platforms the default fsync does not flush the drive cache, and a commit that was uploaded and then lost in a power cut leaves the server ahead of the device, the situation the protocol treats as a restored writer. A full fsync on every commit measured 11 ms against 0.55 ms on a 2026 Mac, so it is paid at checkpoints only (`checkpoint_fullfsync=ON`), and a sync pass checkpoints once before it publishes anything. Both pragmas are per connection, so the verification must cover every connection that writes the store, not only the one that installed the schema. |
| Foreign keys | Keep enabled and verify effective settings for sync-enabled stores. |
| Busy timeout | Retain the current five-second default until contention measurements justify a change. |
| Checkpoints | The checkpoint helper returns the engine's result instead of discarding it. |
| Validation | Run `foreign_key_check` as well as integrity checks in migration validation. |
| Table layout | Protocol tables with composite or text keys are created without a rowid. Model tables keep their rowid; wide-keyed model tables are a measured experiment, not a blanket conversion. |
| Identifiers | Binary identifiers, never hexadecimal text, in protocol tables. |
| Cache and page settings | Tune against representative devices and workloads, not blanket constants. |

Do not switch to `synchronous=NORMAL` merely for throughput: in WAL mode it can lose recently
committed transactions after a power failure. See [SQLite synchronous settings](https://www.sqlite.org/pragma.html#pragma_synchronous),
[`WITHOUT ROWID`](https://www.sqlite.org/withoutrowid.html) and
[foreign-key actions](https://www.sqlite.org/foreignkeys.html#fk_actions).

## Review decisions and delivery order

| Decision | Starting point |
| --- | --- |
| Migration policy for unsupported constraint changes | Rebuild when the declared default can fill the column; refuse otherwise. |
| Participant contract | Change set in, refresh scope out; guard triggers remain for raw SQL. |
| Record shape | Model, key, field group, typed value; binary ids; wire version bump before 1.0. |
| Durable Day-News state | Promote to explicit models; retain cache independence; bound read-state growth. |
| Collection names | Allow duplicates unless a product requirement justifies uniqueness. |
| User-created object IDs | Immutable UUIDs, with an explicit migration for existing identities. |
| Ordering | Text fractional keys with actor tie-break, no rebalancing. |
| Remote atomicity | Document per-record guarantees and tolerate partial arrival. |
| Invalidation granularity | Tables first; keys where measured benefits justify it. |

Implement in the numbered phase order. Keep changes reviewable and validate each layer before
combining them.

Before declaring the integration ready for 1.0, run cross-layer scenarios covering migration
failure and reopening, local and remote rollback, delayed references, cache eviction and
redownload, concurrent edits, restart during publication, and operations split across records.
Measure single edits, bulk read-state changes, collection changes, and cold synchronization
at realistic and larger library sizes. Record statement counts, WAL growth, memory, and
desktop and mobile latency. Synthetic SQL timing alone is insufficient for a performance claim.

## Implementation status

Landed in the working trees under this revision:

- Phase 1 in day-persistence: atomic stage versions, constrained column additions, schema
  comparison with rebuild-or-refuse, foreign-key checks, checkpoint results. The fingerprint
  carries a DDL generation version, so files whose fingerprints an older build recorded get
  one comparison pass; a rebuild carries the table's application triggers across and renames
  without rewriting other tables' references. Rehearsed on a clone of a real Day-News
  library: the four columns the old path left unconstrained were rebuilt, data and adapter
  triggers kept, integrity and foreign-key checks clean, the second open took milliseconds.
- Phase 2, persistence half: the change set and refresh scope in the participant contract;
  day-sync's journal and the Day-News adapter build against it.
- Phase 3 in day-persistence: content-scoped FTS triggers.
- SQLite policy in day-sync: Apple full fsync at checkpoints with one checkpoint before each publish, rowid-free protocol tables; a worker checkpoint job in day-persistence.

Not yet implemented: the typed record and wire version bump, Day-News trigger removal,
durable-state models and growth bounds, ordering keys, batch projection.

No commits, pushes or user-data migrations are made by this plan. After review, incorporate
accepted contracts into the maintained documentation so this planning document does not
become a competing protocol specification.
