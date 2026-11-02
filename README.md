# day-sync

Experimental replication of a [day-persistence](https://github.com/daybrite/day-persistence)
store between devices through a shared folder of immutable objects. MPL-2.0. `publish = false`:
the protocol is under design review, and this repository exists so the design can be tested
without being committed to.

The workspace separates the engine from where the objects live:

| Crate | What it is |
|---|---|
| `day-sync` | The engine: journal, causal order, candidates and projection, the lifecycle, and the [`Store`](crates/day-sync/src/store.rs) trait a transport implements. Ships `MemoryStore` for tests. |
| `day-sync-webdav` | `WebDavStore`: a WebDAV collection as the store, over [day-webdav](https://github.com/daybrite/day-webdav). The `reqwest` feature selects that HTTP provider. |
| `day-sync-filesystem` | `FilesystemStore`: a directory as the store, over `std::fs`. Local, on a mounted share, or in a folder another program carries between machines. |

A store needs seven operations: name itself, make a directory, list one completely, read an
object, write an object, write an object only while its name is free, and delete one. Objects
are immutable and named by their content hash, so every ambiguous outcome is settled by reading
the object back, and a store is never asked to be transactional, keep versions or lock anything.
Anyone can add a transport by implementing the trait.

**Protocol v4 deliberately has no application-layer encryption, signatures, enrollment grants,
private device keys, key rotation, recovery keys, or cryptographic authorization.** The store
and everyone with write access to it are trusted. Over WebDAV, use HTTPS and the account's own
authentication; loopback HTTP remains available for isolated fixtures. TLS and credential
storage are separate from record protection; they have not been removed.

## What it does

- Atomic model edits and durable journal/inbox/outbox; persisted immutable bytes before upload.
- Public UUID space/device identities, canonical bounded JSON, checksums, causal contexts,
  per-actor hash chains, idempotent retries, conflict policies and cloned-writer detection.
- A public device roster for coordinated checkpoints. Roster membership grants no authority.
- Profile and encoding identifiers checked before opening or projecting data.
- Fenced checkpoint handoff, explicit retirement, atomic staging import, backup readback before
  history deletion, and recovery into a fresh space/actor. Receipts are assertions by trusted
  clients, not signatures. A retired-space marker stops cooperating old clients on their next pass.
- Independent valid data can apply while corrupt objects are reported and dependent records wait.

Hashes detect byte corruption and inconsistent histories relative to locally retained evidence.
They **do not authenticate authors, conceal data, or prevent a store or writer from constructing a
new internally consistent history**. A checkpoint hash obtained from the same store is not an
independent security guarantee. A stale backup cannot reveal subsequent hidden writes.

## Known limitations

These are design gaps found in review, not bugs to work around. A failure that matches one of
them says nothing new about the protocol.

- **The record cap is enforced by refusal.** A space holds at most 4,096 records. At the cap a
  device refuses to store a remote record and, through the journal, refuses the user's own edit.
  Two devices that each write their 4,096th record concurrently leave every device unable to
  store one of them, so no common cut exists and no handoff can be attested. Compaction needs the
  coordinated handoff below; there is no automatic compaction.
- **The journal vetoes the whole container.** While a space is quarantined or frozen, or at a
  limit, every local transaction in the application's database fails, including writes to tables
  that are never synchronized. A freeze is irreversible and has no timeout, so one absent device
  keeps every other device read-only until it is explicitly retired.
- **A device restored from backup forks its actor.** Nothing in-band detects the restore. The
  sync pass uploads before it lists, so the first edit after a restore publishes the fork before
  the device could have noticed it, and every device then quarantines the whole space. Persist
  a fresh identity after any restore.
- **Last writer wins by Lamport order, not by time.** The device that syncs more often wins
  conflicts; edits made on a new device before its first sync lose every conflict; a concurrent
  delete beats any concurrent edit. An `Application::resolve` rule overrides the default order.
- **Revision protection is at the candidate level.** An older build that edits an object it holds
  at a stale value writes a record that wins the default order on newer builds too; the newer
  value survives only as a concurrent candidate for the application's resolver.
- **Tombstones are retained across epochs** and count toward the record cap and the checkpoint's
  65,536-object ceiling.
- **One record shape.** Records carry a bookmark-shaped `(url, title)` value; applications encode
  their state into it. There is no field-level merge, partial sync, filtering, or attachments.
- **Polling only, native only.** No push; no scheduler in the library; no `wasm32` target.
- Every listed name costs a worker round trip, every applied record is its own transaction with a
  full cache rescan, and every upload is three store operations.

## Stores

A replica pins the first store it synchronizes with (`Store::endpoint`) and refuses any other
(`sync.endpoint_changed`), so one database never mixes two stores.

**WebDAV.** `WebDavStore::new(client)` wraps a configured `day_webdav::Client`. Collections are
directories, resources are objects, `If-None-Match: *` is the create-only write, and a depth-one
`PROPFIND` is the listing. A Multi-Status that omits the collection itself or reports a member
with a status other than 200 is refused as incomplete rather than read as a shorter listing.
Works with Apache mod_dav, Nextcloud, ownCloud and hosted DAV accounts. Errors arrive as
`StoreError` kinds (`Unauthorized`, `InsufficientStorage`, `Unavailable` with the server's
`Retry-After`, …) with the DAV error attached as the source.

**Directory.** `FilesystemStore::open(path)` uses a directory, creating it if needed; its
canonical path is the endpoint. Objects are written to a hidden temporary file beside their
final name and renamed into place, so a reader never sees a partial object; create-only writes
hard-link the temporary file to the final name, which fails atomically when the name is taken,
and fall back to a check and a rename where the file system has no hard links. Names beginning
with a dot are the store's own and are neither listed nor accepted. Two apps on one machine can
synchronize through a local folder with no server at all; a folder inside iCloud Drive, Dropbox
or Syncthing, or on a mounted network share, carries a space between machines with the caveats
those bring: stale listings and non-atomic create on some mounts, both of which the engine
already treats as a reported condition, never as a deletion.

**Memory.** `MemoryStore::new()` lives in the process, for tests and for an app talking to
itself. It can lose or corrupt an object on demand so a test can watch the engine notice.

## Public identity and protocol boundary

`Space` contains `version: 4`, `encoding: "plain-json-v1"`, a UUID and application profile.
`Identity` contains that descriptor and a device UUID. Persist identity alongside the database;
it is not a secret and belongs outside the keychain. Do not reopen a restored stale database as
an existing writer: recover/rejoin with a fresh identity instead.

Create with `Identity::create_space_for(profile)`; join with `Space::fetch(&store, id)` and
`Identity::join(space)`. The creating client must synchronize once before others can fetch its
space descriptor. Each client registers its public identity automatically. No approval ceremony.

`envelope.rs` is the single bounded record encode/decode boundary, separate from logical
transactions, transport and application projection. Unsupported versions/encodings are refused.
A future protected format must use a separately reviewed codec/key-provider design and a new
space format; it must never silently fall back to plaintext. There is no placeholder encryption
feature, dormant crypto implementation or runtime algorithm negotiation in this version.

A space lives in the directory `day-sync-v4-<uuid>` with records `tx-<actor>.<seq>.<hash>.ds4`,
the descriptor `space.json`, one `member-<actor>.json` per device and, after a handoff,
`retired.json`. Checkpoint packages are `DAYPKG04` and are published under
`day-handoff-v4-<uuid>/cp-<hash>/`. Encrypted v2/v3 stores and packages are rejected without
migration. [docs/experiment-history.md](docs/experiment-history.md) records how the protocol
got here.

## Record revisions

An application declares `revision()`, the highest record revision it reads and writes (default 1),
and `requires(change)`, the lowest revision that can read a change. Every revision must keep
reading all lower ones; the profile name never changes for this. The journal gives each
transaction one required revision, so a local commit mixing old and new kinds of change becomes
several records and older builds keep syncing the changes they understand. Transactions carry
`requires` and the writer's `revision`, both omitted when 1, so first-revision records keep their
bytes.

A build that reads a lower revision stores such a transaction and advances causal order past it,
but neither validates nor projects it. Each object it touches is held: the build resolves only the
candidates it can read, so devices at one revision agree, or keeps its value when it can read
none. `SyncReport` and `Snapshot` report `held` and `required_revision`; holding is not an error.
A transaction supersedes only observed candidates its writer's revision could read, so the two
stay concurrent candidates (see the known limitations for what the default order then does).
After an upgrade, the next `apply_ready` validates and projects every held object the build can
now read, with no download. A device that holds anything refuses to attest a checkpoint
(`sync.checkpoint_requires_upgrade`).

## Checkpoints, backups and limits

Freeze all retained devices, exchange pending records/rosters, collect matching receipts, then
`prepare_checkpoint` into an isolated fresh target. `Package::publish(&store, &pin)` writes the
baseline objects before the manifest; read back a durable off-store plaintext package before
`collect_history`. Keep the pin with your backup. `Package::recover` creates a new identity;
persist it before seeding. After interruption, `recover_to` resumes using that same persisted
identity. It rejects reusing either old space.

The package has a 64 MiB manifest/object budget plus bounded framing, counts and individual
fields. `write_to/read_from` support bounded incremental I/O; convenience exports and the decoded
package still hold data in memory. Backups contain readable application data.

Limits: 4,096 transaction objects per epoch, 32 devices, 128 changes and 256 KiB per record,
65,536 changes per local batch, 2 MiB manifest and 64 MiB package. Stop admitting new devices
while coordinating a handoff: no store provides a transactional join/checkpoint lock. A retired
device's unsent edits need explicit salvage. Day-News has no automatic cutover UI, and
local-only archives must be carried separately.

## Development

The workspace depends on day-persistence and day-webdav from their repositories until those
crates are published. To build against the sibling checkouts, write the machine-local patch
table once (it is gitignored):

```sh
day patch --local ../day --local ../day-persistence --local ../day-webdav
```

Then, from this directory:

```sh
cargo test --workspace --features day-sync-webdav/reqwest
cargo clippy --workspace --all-targets --features day-sync-webdav/reqwest -- -D warnings
cargo fmt --all -- --check
```

CI runs the same through the shared Day library workflow on Linux, macOS and Windows, with and
without the `reqwest` provider. There is no demo app, so the workflow's demo and website legs
are skipped.

The engine's own tests (`crates/day-sync/tests`) cover out-of-order/duplicate delivery, fork
detection, causal validation, corrupt records and a full sync pass through `MemoryStore`. The
WebDAV crate's tests cover uncertain writes, partial roster uploads, publication/GC failures,
checkpoint bounds, devices at three record revisions and process termination during journaling,
freeze, import and recovery, against an in-process simulated server and the day-webdav demo
server. The filesystem crate's tests run two apps and a checkpoint handoff through a temporary
folder. The revision scenario also runs against a fixture with
`DAV_FIXTURE_URL=… cargo test -p day-sync-webdav --features reqwest --test revisions -- --ignored`;
set `DAY_SYNC_REQWEST=1` to use the reqwest provider. Ignored external fixtures are run
explicitly through `scripts/interop.py` (which finds the day-webdav checkout beside this one, or
at `DAY_WEBDAV`) and Day-News's `scripts/webdav-shakeout.py`. Do not run private dataset tests
against a public or shared server. Fresh fixture namespaces contain plaintext, and a failed test
may leave those namespaces behind.
