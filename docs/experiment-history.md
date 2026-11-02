<!--
Copyright © The Daybrite Project
SPDX-License-Identifier: MPL-2.0
-->

# Experiment history

Dated notes on how the prototype reached protocol v4. They record decisions, not the current
contract; the README and the source are authoritative. The pages, diagrams and captured
evidence of the earlier milestones were working material and did not move into this repository.

## October 2026: plaintext replication over WebDAV (v1)

The first milestone proved the shape that still stands: a `TransactionParticipant` installed on
the application's `ModelContainer` journals every local commit as a bounded canonical record
inside the same SQLite transaction, with the replica's sequence, a causal context and the
per-object candidates. A durable inbox separates network receipt from application; remote
imports project under `WriteOrigin::RemoteImport` so they never echo back out. Crash tests
terminated the process at every boundary of a sync pass and the store reopened consistent.

## October 2026: authenticated encryption and enrollment (v2, v3)

v2 sealed each record with XChaCha20-Poly1305 and signed it with a per-device Ed25519 key, and
added an administrator-signed, append-only membership chain with an enrollment ceremony. v3
added the fenced lifecycle (freeze, attest, prepare a checkpoint into a fresh root, publish,
install, collect history, recover) with signed cuts and encrypted recovery capsules. An external
review found the cryptographic surface too large to qualify for the problem at hand: key
custody, revocation, rotation and restored-device recovery were all open, and the server was
still trusted for availability.

## 29 October 2026: back to plaintext under a trusted server (v4)

Encryption, signatures, enrollment grants and recovery keys were removed. The server and every
account holder are trusted; HTTPS and the account's own authentication protect the bytes in
transit and at rest. Hashes remain as corruption checks and as the per-actor chain. Encrypted
stores and packages are refused without migration. The lifecycle survived the change with its
coordination intact, which is why `freeze` still exists even though the threat it was built
against no longer does; see the README's known limitations.

## October 2026: the Day-News application profile

`Replica::open_application` installs an `Application` adapter on the app's own container. The
adapter captures pending local intentions (subscriptions, read state, stars) as bookmark-shaped
records, validates every payload, resolves concurrent candidates with a domain rule (unread,
starred and unsubscribed win) and projects winners in the same transaction. It is an explicit
adapter seam, not arbitrary-schema replication, and it is the only consumer of this crate.

## 31 October 2026: record revisions

Transactions carry the writer's `revision` and the `requires` revision of their changes, both
omitted when 1 so first-revision records keep their bytes. A build that reads a lower revision
stores such a record, advances past it, and holds each object it touches at its last readable
value until a build that reads it opens the store. A writer supersedes only the candidates its
revision could read, so the newer value stays a concurrent candidate rather than being replaced.

## 2 November 2026: own repository

The crate left `day-persistence/crates/` for a repository of its own, depending on
day-persistence and day-webdav from their repositories until they are published. The
persistence seam it needs (`TransactionParticipant`, `WriteOrigin`, `write_with_origin`) is
part of day-persistence and documented there.

## 2 November 2026: the store contract

The engine stopped naming WebDAV. Everything it needs from a remote is seven operations on a
directory of immutable objects (`Store` in `crates/day-sync/src/store.rs`): name the store,
make a directory, list one completely, read, write, write-if-absent, delete. `day-sync-webdav`
carries the WebDAV implementation and its fault and interop tests; `day-sync-filesystem` adds a
directory implementation over `std::fs`, which lets two apps on one machine synchronize with no
server and runs the lifecycle through a folder; `MemoryStore` serves the engine's own tests.
Store failures are classified (`StoreErrorKind`) with the transport's error attached, so an
application schedules retries without knowing which transport it runs on.
