# Application records and media libraries

An application profile defines a logical library. Each installation joins its space with a fresh actor and stores that identity with its local database. Stanza Redux discovers one library per DAV collection using a create-only descriptor at `stanza-redux-v1/library.json`. Two devices enrolling concurrently read back the descriptor and join the same space.

## Mapping a database

Use `RecordKey { model, key, field }` to identify an independently editable value. Include the profile namespace in the model. Keep the key stable across devices and renames. Encode values with `change(Some(&value))`, delete with `change::<T>(None)`, and check identities and types with `decode` in the application adapter. These records reuse v5 causality, revision handling, deletion markers and compaction. Each encoded key and payload has an 8 KiB limit. Put larger content behind an asset reference.

Capture each edit in the same SQLite transaction as its durable intent. An app using another local SQLite connection can install transactional capture triggers, drain its outbox through the replica worker, and implement `before_import` to defer incoming history while a local intent is pending. Suppress capture during projection, retain child records until their parent arrives, and enforce the freeze fence on all writers. Use `candidates` and `choose_candidate` when an app lets the user resolve concurrent values. A choice commits only while the versions displayed still match.

## File transfer

The `assets` module uses a sibling collection named `day-assets-v1-<space UUID>`. A whole-file SHA-256 digest and byte length identify an asset. Files up to 4 MiB occupy one object. Larger files use ordered 4 MiB chunks and a final short chunk. The immutable manifest contains the ordered chunk hashes. Its digest is part of the attachment reference. The current format supports up to 65,536 chunks per file.

| Path below the media collection | Contents |
|---|---|
| `objects/<first two hex digits>/<sha256>` | Original file or chunk bytes |
| `manifests/<first two hex digits>/<sha256>` | JSON array of lowercase SHA-256 digests |

Queue a stable local source before uploading. Publish the reference in an application transaction only after `upload` or `upload_with_journal` succeeds. A lost PUT response is reconciled by readback. An optional local SQLite `UploadJournal` stores verified strong validators, bound to the endpoint, space, kind and digest. Strongly validated conditional reads can reuse those receipts across launches. Servers lacking a usable strong validator receive full verification reads.

For downloads, pass a read/write staging file to `download`. Verified local chunks survive interruption. Install the completed file with a same-filesystem rename after whole-file verification. Keep the local path, pin state, transfer journal, covers, and search indexes device-local. Changing a bookmark or playlist entry incurs no media upload. See the Stanza adapter for EPUB, descriptive metadata, and reading-position records.

## Retention and recovery

Ordinary metadata compaction and local cache eviction leave remote media intact. A coordinated checkpoint can retire the old space. Implement `before_freeze` to drain attachment work and `asset_references` to enumerate every attachment from a surviving candidate. Finish the ordinary handoff protocol and verify the published retirement marker.

For a retired space, `collect_checkpoint_assets` verifies the matching cut, republishes and reads back the checkpoint at an independent backup endpoint, backs up recognized source objects by digest, and copies assets from all surviving candidates into the destination space. Only then does it reclaim a bounded number of old media objects. A retry repeats verification. Use the same backup collection for every cleanup pass. The backup needs independent storage with enough room for the old pool. A different endpoint string alone cannot establish physical independence. The library-level coordinator owns destination activation and updating its discovery descriptor. `assets::copy` also supports restoring an individual file to a fresh space.

The trusted server and authorized clients can read the content. Asset hashes detect damaged bytes. Metadata snapshots and a SQLite backup alone cannot restore missing EPUBs or other media.
