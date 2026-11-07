<!--
Copyright © The Daybrite Project
SPDX-License-Identifier: MPL-2.0
-->

# Protocol and integration reference

Day-sync replicates application state through a shared object store. This reference describes
protocol v5, its integration interfaces, and the conditions for publication, cleanup, and recovery.
For the model and illustrated scenarios, read [How synchronization works](synchronization.md).

The crates are experimental and are not published for production use. Older protocol stores,
databases, and packages are rejected; this implementation provides no automatic migration.

## Open and identify a replica

A **space** identifies a shared dataset and its application profile. An **actor** identifies a
writer's ordered history. Each replica persists a public `Identity` containing both descriptors
alongside its database. These identifiers are not secrets or authorization credentials.

Create a space with `Identity::create_space_for(profile)`. The creating device must sync once
before another device can fetch its descriptor with `Space::fetch(&store, id)`. Join that space
with `Identity::join(space)` and persist the returned identity before using it. Keep each writer's
identity unique; copying an identity into independently writable databases can create a fork.

`Replica::open(path, identity)` opens the built-in bookmark model. For an application's database,
use `Replica::open_application` with an `Application` adapter and a container factory. The factory
must register `Bookmark` alongside the application models. Opening installs the journal before
writers can access the container. A database-file lock rejects a competing open with `Error::Busy`.

The first sync pins the storage endpoint to the database. A different endpoint returns
`Error::WrongEndpoint`; changing storage requires an explicit application recovery or migration
procedure. Call `Replica::close` when finished with the database worker.

See the [identity implementation](../crates/day-sync/src/identity.rs) and
[replica API](../crates/day-sync/src/lib.rs) for signatures.

## Map application data

The [`Application` trait](../crates/day-sync/src/application.rs) maps app data to the bounded
`Change` format: an object identifier and an optional `BookmarkValue` with `url` and `title` fields.
A missing value represents deletion. This is an explicit mapping, not arbitrary-schema replication.

| Method | Contract |
| --- | --- |
| `profile` | Identifies the application format; it must match the space and stored database profile. |
| `install` | Installs the adapter's schema and capture mechanism. |
| `capture` | Returns local intentions inside the committing model transaction. It receives the ORM's folded change set for that transaction: each inserted, updated (with its changed columns), deleted or rewritten row, with the author tags of those edits. Rows the engine's own `ON DELETE` removed without being faulted are not in it. |
| `projected_tables` | Names the model tables `capture` and `project` write by SQL, so only their resident rows are re-read after a local commit; the default re-reads every table. |
| `validate` | Checks incoming readable payloads, including namespace, identity, and canonical application encoding. |
| `resolve` | Selects a candidate index deterministically; it must be pure. |
| `project` | Writes the chosen value into the application model inside the import transaction. |
| `revision`, `requires` | Declare the writer's readable revision and each change's minimum readable revision. |
| `held` | Reports unreadable surviving state after projection, or `None` when the hold clears. |

Capture and projection must not echo remote imports into new local changes. The adapter must
suppress its capture triggers during projection and keep user state separate from disposable caches.
A frozen or quarantined replica can veto transactions throughout the participating container,
including writes to tables outside the synchronized model.

The default resolver orders candidates with deletions first, then descending logical counter,
actor identifier, and sequence. This is not wall-clock “last edit wins.” Choosing a candidate for
display leaves the other concurrent candidates intact. A subsequent write supersedes only the
observed candidates its writer's revision can read.

### Application revisions

`revision()` defaults to 1 and must retain support for every lower revision. `requires(change)`
defaults to 1 and must be between 1 and the writer's revision. Mixed local batches are split into
records so readable changes can continue to synchronize with older builds.

An older build retains unreadable candidates and advances their received history, but does not
apply application validation or projection to those values. It resolves readable candidates,
or retains a local display cache if none are readable. A fresh replica may have no such cache.
`held` and `required_revision` expose this condition; it is not a deletion or a sync failure.
After an upgrade, `apply_ready` validates and projects newly readable values without downloading
them again. A replica holding unreadable state cannot attest a checkpoint.

## Drive synchronization

The application calls `sync` or `sync_with_options`, handles errors, and schedules later attempts.
Sync connects to the pinned store, validates available snapshots and record evidence, applies
ready work, publishes pending local state, and performs eligible maintenance. Large backlogs can
interleave catch-up and maintenance. Visible fork evidence is checked before publication or cleanup
can discard it. Concurrent `sync`, `compact`, and `freeze` calls on the same replica family return
`Error::Busy`; local edits may continue during an unfrozen sync.

`compact` and `compact_with_options` force a full snapshot audit and publish current state when
needed. An unchanged, fully audited snapshot with no pending journal or eligible inputs needs no
publication. Every resumed cleanup journal still requires a fresh complete verification.

| `SyncOptions` field | Default | Effect |
| --- | --- | --- |
| `verify_snapshots` | `false` | Force a full audit of known snapshots on this sync attempt. Explicit compaction always audits. |
| `max_deletions` | `128` | Bound remote DELETE requests per call, including requests for already absent inputs. Zero permits publication without deletion. |

### Interpret progress

An `Ok(SyncReport)` can represent bounded progress rather than an empty outbox or complete cleanup.
Schedule another attempt when `pending_deletions > 0`, `downloads_deferred`, or `uploads_deferred`.
Avoid a tight retry loop when storage or local capacity prevents progress.

| Report fields | Meaning |
| --- | --- |
| `uploaded` | Individually reconciled outbox records, including exact bytes already present remotely. |
| `downloaded`, `applied`, `waiting` | Record download/application counts and unapplied rows still waiting locally. |
| `snapshots_verified` | Successful full snapshot reads during the scan. |
| `snapshots_published`, `published_snapshot` | Publications or journal revalidations in this call, and the replacement's name. |
| `deletion_requests`, `objects_reclaimed` | DELETE attempts and responses reporting an existing object was removed. |
| `pending_deletions` | Inputs still outstanding in the saved cleanup journal. |
| `downloads_deferred`, `uploads_deferred` | Incoming or outgoing work deferred to another call to respect maintenance bounds. |
| `compaction_capacity_limited` | Local state encoding exceeds the snapshot budget; automatic maintenance can fall back to ordinary records. |
| `held`, `required_revision` | Unreadable surviving application state and the revision needed to read it. |
| `repaired_history` | A published replacement covers lost or corrupt known snapshot evidence. |

Errors return `Error`, not a partial report. Independent valid changes and durable progress may
already have committed. `Snapshot.pending_uploads` exposes the current outbox independently of a
particular attempt's report.

## Publish snapshots and reclaim inputs

A snapshot contains surviving candidates, deletion entries, revision metadata, and each actor's
sequence, hash, and logical counter. An incoming snapshot joins this state with local state;
coverage alone does not publish a local unsent suffix or authorize raw-record deletion. Equal
histories with different candidate states and joins that forget known objects are rejected.

The [publication implementation](../crates/day-sync/src/compaction.rs) saves the exact replacement
bytes and deletion list in SQLite before network publication. It writes parts before the manifest,
then validates the complete replacement through the normal reader. Verification publishes only
the captured local prefix; newer edits remain pending. Eligible deletion inputs are individually
read and applied records, or individually verified older snapshots, covered by the replacement.
Progress is persisted after batches of at most 32 inputs. A retry repairs and revalidates the
saved replacement before continuing, regardless of an earlier verification flag.

Automatic maintenance is due at 64 retained applied records or 64 individually applied input
receipts. A separate 4,096-record threshold triggers maintenance during incoming catch-up.
These are maintenance thresholds, not lifetime record limits. Listed pending outbox objects are
reconciled before compaction so their exact receipts survive publication. A large unsent outbox
can publish directly as a snapshot before uploading individual records.

A call publishes or revalidates at most one captured snapshot. Resuming an older journal can leave
64 or more newer outbox records pending; `uploads_deferred` then requests another call. With
`max_deletions = 0`, ordinary record publication continues instead of waiting indefinitely for an
old journal whose cleanup cannot finish. A frozen replica does not compact.

### Audit and repair

Unknown snapshots receive full validation. A known loaded snapshot may reuse its durable receipt
when every part and manifest name is listed and fewer than 16 scan attempts have elapsed since
verification. Missing names prompt investigation immediately. Changed bytes under unchanged names
can remain undetected until the next audit. Failed audits do not refresh the receipt.

Known applied state or a saved journal may repair lost remote data. A replacement must cover the
lost state before repair is reported as complete. A frozen or capacity-limited replica can still
report missing history. Missing published raw history retains `RemoteHistoryMissing` on the
attempt that discovers it, even when applied state is republished; retry confirms recovery.
Missing unapplied predecessors are never invented.

An incomplete older snapshot can remain as residue when valid state covers it. Its header grants
no deletion authority. Covered records that reappear are ignored unless exact retained evidence
requires validation. Unobserved uploads and abandoned parts may remain until coordinated epoch
collection. Compacted history cannot supply every historical fork check available from full records.

## Handle failures

| Result | Application response |
| --- | --- |
| `Busy` | Serialize the operation or close a competing database open, then retry. |
| `Store(error)` | Inspect the error kind and `retry_after`. Restore credentials or capacity where needed; respect server backoff. |
| `RemoteHistoryMissing` | Investigate missing data and retry when a surviving replica or backup can supply it. |
| `RemoteObjectsRejected`, `Integrity`, `InvalidWire` | Report invalid remote data; preserve local state and investigate the store or writer. |
| `Fork` | Stop using the conflicting identity and recover with a fresh identity. Quarantine is persistent. |
| `WrongEndpoint`, `WrongSpace`, `UnsupportedFormat` | Correct the configuration or use an explicit recovery path; repeated retries do not change compatibility. |
| `Retired` | Follow the application's handoff or salvage procedure for the old space. |
| `Limit` | Inspect the operation's resource bound. Only local automatic snapshot encoding has the nonfatal capacity-report path. |
| `Database(error)` | Preserve the database and inspect the underlying validation, lifecycle, or persistence error. |

Transport and remote verification limits remain errors. They are not reported as local snapshot
capacity. A readback proves present readability, not future retention or author authenticity.

## Store contract and files

[`Store`](../crates/day-sync/src/store.rs) supplies endpoint identity, directory creation, complete
listing, read, write, create-if-absent, and delete. A listing that cannot be vouched for must fail
as incomplete. Eventual visibility is still required for convergence; a fresh device cannot infer
entirely hidden history from an empty view.

[`WebDavStore`](../crates/day-sync-webdav/src/lib.rs) uses a WebDAV collection. The `reqwest` feature
enables that HTTP provider. [`FilesystemStore`](../crates/day-sync-filesystem/src/lib.rs) uses a
canonical directory path as its endpoint and publishes files through temporary-file replacement.
Mounted or replicated folders retain their provider's visibility and durability constraints.
`MemoryStore` supplies an in-process implementation for tests.

Records and snapshot parts use content-derived immutable names. Repair under a persisted name
writes the same expected bytes. Metadata such as the space descriptor and retirement marker has
fixed names; not every file is content-addressed.

| Object | Location or format |
| --- | --- |
| Active space | `day-sync-v5-<uuid>/` |
| Descriptor and roster | `space.json`, `member-<actor>.json` |
| Record | `tx-<actor>.<sequence>.<hash>.ds5` |
| Snapshot | `snap-<state-hash>.ds5` and `snap-<state-hash>.part-<index>.<part-hash>.ds5` |
| Genesis | A separately encoded baseline with `genesis-` manifest and part names. |
| Retirement | `retired.json` |
| Checkpoint package | `DAYPKG05`, published under `day-handoff-v5-<uuid>/cp-<hash>/` |

The descriptor uses version 5 and encoding `plain-json-v1`. Profiles must match across participants.
Hashes check content and retained history evidence; they do not encrypt data, grant membership
rights, or authenticate a writer. Use trusted storage access controls and HTTPS for WebDAV.

## Checkpoints and recovery

Ordinary compaction stays in the same space and retains conflict candidates. A **checkpoint** is a
coordinated terminal handoff to a fresh space with a resolved baseline. It is a separate lifecycle
operation, implemented in [the package and handoff API](../crates/day-sync/src/lifecycle.rs).

1. Persist the fresh target identity and create an isolated staging database.
2. Freeze every retained device using the same token, then synchronize their pending work and
   complete listings. Freeze is irreversible and has no timeout-based thaw.
3. Collect matching `attest` receipts and explicitly identify any retired devices. Stop admitting
   new devices during coordination; unsent changes on excluded devices require separate salvage.
4. Call `prepare_checkpoint`, obtain its `CheckpointPin`, and publish the package. Import into the
   staging target with `install_checkpoint` as needed; expose it only after validation completes.
5. Store an independent backup, read it back, and obtain `VerifiedBackup` with `verify_backup`.
   Only then call `collect_history`, which also fetches and verifies the published package before
   deleting the old paths enumerated in the handoff.

The application is responsible for placing the backup off the shared store. `VerifiedBackup`
proves that package bytes verified; it cannot prove where they were stored. Keep the pin with the
backup. The package and backup contain plaintext application data.

`Package::recover` with `RecoveryPolicy::AcceptLossAfterCheckpoint` creates a fresh space and
writer identity. Persist that identity before calling `Recovered::seed`. After interruption,
`recover_to` reuses the persisted recovery identity. Recovery accepts loss of changes after the
checkpoint; it does not repair a stale writer by reusing its identity.

A target starts with a chunked genesis baseline that includes deletion entries and revision
metadata. Its hash anchors sequence 1 for the bootstrap actor. That actor's first ordinary record
is sequence 2; other actors begin at 1. Ordinary snapshots can later replace the baseline.

## Resource limits

These bounds protect local work and encoded formats. A valid workload can exceed them. Snapshot
publication also needs remote space for the complete replacement before input reclamation.

| Resource | Limit |
| --- | --- |
| Actors in a space | 32 |
| Retained transaction rows per replica | 65,536 |
| Changes per ordinary local batch / record | 65,536 / 128 |
| Logical record bytes | 256 KiB; the versioned envelope permits a further 2 KiB |
| Visible names per listing | 139,264 |
| Snapshot or genesis state / part | 64 MiB / 4 MiB |
| Snapshot manifest | 64 KiB |
| Checkpoint manifest / package payload | 40 MiB / 112 MiB, with separately bounded framing |
| Application revision | 1–65,535 |

Field and identifier validation adds further bounds in the [wire](../crates/day-sync/src/wire.rs),
[state](../crates/day-sync/src/state.rs), and [package](../crates/day-sync/src/lifecycle.rs) code.
Snapshot encoding and verification hold substantial state in memory. Tombstones and surviving
concurrent candidates remain across ordinary compaction; the checkpoint baseline also retains
deletion entries. There is no automatic tombstone collection.

## Development

The workspace uses the sibling day-persistence and day-webdav repositories. For local sibling
checkouts, generate the ignored Cargo patch configuration once:

```sh
day patch --local ../day --local ../day-persistence --local ../day-webdav
```

From the repository root, run the example and checks:

```sh
cargo run -p day-sync --example compaction
cargo test --workspace
cargo test -p day-sync-webdav --features reqwest
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

The [example](../crates/day-sync/examples/compaction.rs) uses disposable synthetic data. Tests cover
concurrent edits, independent replay, quota boundaries, corrupted and missing objects, interrupted
uploads, process restarts, and cleanup verification. See the
[recovery scenarios](../crates/day-sync/tests/recovery.rs),
[fault schedules](../crates/day-sync/tests/faults.rs), and
[WebDAV request tests](../crates/day-sync-webdav/tests/compaction.rs).

The engine suite runs once with the default features. The additional `reqwest` pass targets
only the WebDAV crate, since changing the HTTP provider does not change the engine. Windows
runs 2,500 steps of the long randomized engine schedule and eight complete restart/transport
schedules; other hosts run 10,000 steps and 32 schedules. The shorter Windows runs retain
full durability and replay checks while reducing the risk of CI timeouts.

The opt-in [measurement test](../crates/day-sync/tests/measurements.rs) reports synthetic storage
costs; it does not measure production network performance:

```sh
cargo test -p day-sync --test measurements -- --ignored --nocapture
```

External-provider tests require disposable isolated fixtures. The
[interop script](../scripts/interop.py) describes the Apache and Nextcloud setup; do not point it
at a shared account. Real provider behavior, sustained network performance, memory use, and
application recovery flows still need release validation. CI checks host builds with and without
`reqwest` and cross-target compilation. Browser execution is not implemented.
