<!--
Copyright © The Daybrite Project
SPDX-License-Identifier: MPL-2.0
-->

# Automerge experiment and decision

**Decision, 6 November 2026:** retire the Automerge prototype from day-sync and retain the
filesystem/WebDAV architecture. The prototype demonstrated viable replication; it did not
provide a better overall fit for this package or for Day-News. This is an architectural
decision, not a finding that Automerge is unreliable.

## What we tried

The experimental `day-sync-automerge` crate paired an Automerge document with a day-persistence
SQLite database. An application adapter captured local changes and projected incoming changes
in the same database transactions, preserving atomicity between application state and sync
history. Each database used one document with stable scalar field keys and explicit tombstones.
Applications still supplied their schema, validation, conflict preferences, and initial import.

A Rust client exchanged repository messages over WebSockets with the unmodified, generic
`automerge-repo-sync-server`. No application-specific server implementation or JavaScript
runtime in the client was needed. Tests also used an independent JavaScript client.
The recorded versions were Rust Automerge 0.10.0, reference server 0.3.0, JavaScript Automerge
3.5.0, and Repo/network/storage adapters 2.5.6.

Day-News piloted subscription and read/star synchronization in isolated macOS, iOS, and Android
variants, first with a local server and then the public demo server. The networking adapter was
made pluggable: the `day-http` transport used day-part-http's native WebSockets, while standalone
tools could use Tokio Tungstenite. Day-News subsequently returned to WebDAV-based day-sync.

## What the experiment established

The recorded validation covered offline edits, concurrent field choices, duplicate delivery,
partitions, restart, transaction rollback, process crashes, corrupt state, pending dependencies,
and database copies. Reference-server tests covered Rust/JavaScript interoperability, concurrent
clients, persisted server restart, and restoring an empty server from a surviving client.
Protocol tests exercised malformed messages, disconnection, refusal, and stalled handshakes.
These results establish the prototype's tested behavior, not production certification.

Same-value assignments required care. Automerge can treat them as no-ops, and resolving a
conflict to an existing winner need not create a fresh causal decision. We accepted no-op
semantics for Day-News and tested the three-device consequences. Applications requiring an
explicit reaffirmation would need to represent that intent separately.

Synthetic measurements showed practical performance on the development Mac, including very
small documents for repetitive values. They were not a controlled comparison with day-sync
and did not establish bounds for realistic multi-year libraries, memory use, or mobile energy
consumption. Saving a consolidated document reduced replay work but retained its history.

## Why we did not keep it

- **Storage independence is a central requirement.** The tested repository protocol needs an
  Automerge-capable server. The generic reference server meets that need, but an ordinary
  WebDAV server or mounted folder does not implement its handshake and message exchange.
  Transporting Automerge files through such storage is possible in another design; it would
  still require discovery, exchange, recovery, and history-management decisions.
- **Day-News primarily shares small choices.** Subscriptions, collections, read flags, and
  stars suit field-level reconciliation. Article content comes from publishers. The prototype
  did not use collaborative text or list editing, and still needed an application adapter
  and the same transactional persistence safeguards.
- **It added an operational system to maintain.** Production use would need server deployment,
  authentication, document authorization, enrollment, backups, and lifecycle handling. The
  reference/demo setup did not supply our production privacy or durability requirements.
  Its acknowledgement of document heads was not an fsync receipt. Existing WebDAV account
  infrastructure better matches the deployment model we want.
- **The prototype left substantial work unfinished.** It had no history truncation or total
  history/memory quota, used whole-document validation and serialization paths, and lacked
  v5's per-record handling of future application revisions. A 16 MiB wire-message limit and
  per-call budgets did not provide those missing bounds.

The pilot opened a connection for each bounded sync call. Frequent reconnects were a property
of that implementation, not a requirement of Automerge: a persistent connection could deliver
incoming changes promptly. Doing that well would require connection ownership, reconnection,
mobile suspension, and background-execution policies. It would improve the pilot's networking
without removing the storage and operational trade-offs above.

## What remains

day-sync consists of the replication engine and filesystem/WebDAV stores described in
[How synchronization works](synchronization.md) and the
[protocol reference](protocol-reference.md). This decision does not remove their costs:
applications must still define identities and conflict rules, schedule sync, handle failures,
and maintain backups. Folder/WebDAV replication is not a live collaboration service; separate
media-transfer and cache policies are needed for applications sharing books, music, or images.

The prototype crate, Node test harness, dependency declarations, and dedicated CI workflow
were removed. A verified local source archive preserves the uncommitted experiment, its
original technical notes, tests, and lockfiles; it is outside the repository and is not a
published dependency. Revisit Automerge as a separate integration if an application's
collaborative data model and willingness to operate a sync service justify it.
