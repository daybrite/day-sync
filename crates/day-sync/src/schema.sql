-- Copyright © The Daybrite Project
-- SPDX-License-Identifier: MPL-2.0
-- Experimental plaintext schema/wire v4. Trusted server and writers; no prototype migration.
CREATE TABLE IF NOT EXISTS _day_sync_config (
 id INTEGER PRIMARY KEY CHECK(id=1), version INTEGER NOT NULL CHECK(version=4),
 space TEXT NOT NULL, actor TEXT NOT NULL, next_sequence INTEGER NOT NULL CHECK(next_sequence>0),
 clock INTEGER NOT NULL CHECK(clock>=0), endpoint TEXT, poisoned INTEGER NOT NULL DEFAULT 0 CHECK(poisoned IN (0,1))
);
CREATE TABLE IF NOT EXISTS _day_sync_profile(id INTEGER PRIMARY KEY CHECK(id=1),name TEXT NOT NULL);
-- Experimental fenced-epoch lifecycle. No encryption or signing keys exist.
CREATE TABLE IF NOT EXISTS _day_sync_fence(id INTEGER PRIMARY KEY CHECK(id=1),token TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _day_sync_handoff(id INTEGER PRIMARY KEY CHECK(id=1),hash TEXT NOT NULL,manifest BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS _day_sync_bootstrap(id INTEGER PRIMARY KEY CHECK(id=1),hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _day_sync_recovery(id INTEGER PRIMARY KEY CHECK(id=1),hash TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS _day_sync_guard (id INTEGER PRIMARY KEY CHECK(id=1), mode INTEGER NOT NULL CHECK(mode IN (0,1,2)));
INSERT OR IGNORE INTO _day_sync_guard VALUES(1,0);
CREATE TABLE IF NOT EXISTS _day_sync_dirty (object TEXT NOT NULL PRIMARY KEY);
CREATE TABLE IF NOT EXISTS _day_sync_transactions (
 actor TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>0), hash TEXT NOT NULL UNIQUE,
 bytes BLOB NOT NULL CHECK(length(bytes)<=264192), lamport INTEGER NOT NULL CHECK(lamport>0),
 local INTEGER NOT NULL CHECK(local IN (0,1)), applied INTEGER NOT NULL CHECK(applied IN (0,1)),
 published INTEGER NOT NULL CHECK(published IN (0,1)), PRIMARY KEY(actor,sequence),
 CHECK(local=0 OR applied=1), CHECK(local=1 OR published=0)
);
CREATE INDEX IF NOT EXISTS _day_sync_outbox ON _day_sync_transactions(sequence) WHERE local=1 AND published=0;
CREATE INDEX IF NOT EXISTS _day_sync_inbox ON _day_sync_transactions(lamport,actor,sequence) WHERE applied=0;
CREATE TABLE IF NOT EXISTS _day_sync_frontiers (
 actor TEXT NOT NULL PRIMARY KEY, sequence INTEGER NOT NULL CHECK(sequence>0),
 FOREIGN KEY(actor,sequence) REFERENCES _day_sync_transactions(actor,sequence)
);
CREATE TABLE IF NOT EXISTS _day_sync_candidates (
 object TEXT NOT NULL, actor TEXT NOT NULL, sequence INTEGER NOT NULL,
 value BLOB, PRIMARY KEY(object,actor,sequence),
 FOREIGN KEY(actor,sequence) REFERENCES _day_sync_transactions(actor,sequence)
);
CREATE INDEX IF NOT EXISTS _day_sync_candidate_transaction ON _day_sync_candidates(actor,sequence);
CREATE TRIGGER IF NOT EXISTS _day_sync_insert BEFORE INSERT ON sync_bookmarks BEGIN
 SELECT CASE WHEN coalesce((SELECT mode FROM _day_sync_guard WHERE id=1),0) NOT IN (1,2) THEN RAISE(ABORT,'sync requires instrumented transaction') END;
END;
CREATE TRIGGER IF NOT EXISTS _day_sync_update BEFORE UPDATE ON sync_bookmarks BEGIN
 SELECT CASE WHEN coalesce((SELECT mode FROM _day_sync_guard WHERE id=1),0) NOT IN (1,2) THEN RAISE(ABORT,'sync requires instrumented transaction') END;
 SELECT CASE WHEN NEW.id != OLD.id THEN RAISE(ABORT,'sync identity is immutable') END;
END;
CREATE TRIGGER IF NOT EXISTS _day_sync_delete BEFORE DELETE ON sync_bookmarks BEGIN
 SELECT CASE WHEN coalesce((SELECT mode FROM _day_sync_guard WHERE id=1),0) NOT IN (1,2) THEN RAISE(ABORT,'sync requires instrumented transaction') END;
END;
CREATE TRIGGER IF NOT EXISTS _day_sync_note_insert AFTER INSERT ON sync_bookmarks BEGIN
 INSERT OR IGNORE INTO _day_sync_dirty VALUES(NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS _day_sync_note_update AFTER UPDATE ON sync_bookmarks
 WHEN NEW.url IS NOT OLD.url OR NEW.title IS NOT OLD.title BEGIN
 INSERT OR IGNORE INTO _day_sync_dirty VALUES(NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS _day_sync_note_delete AFTER DELETE ON sync_bookmarks BEGIN
 INSERT OR IGNORE INTO _day_sync_dirty VALUES(OLD.id);
END;

CREATE TABLE IF NOT EXISTS _day_sync_forks (
 hash TEXT NOT NULL PRIMARY KEY, actor TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>0),
 bytes BLOB NOT NULL CHECK(length(bytes)<=264192)
);

CREATE TABLE IF NOT EXISTS _day_sync_space(id INTEGER PRIMARY KEY CHECK(id=1), descriptor BLOB NOT NULL CHECK(length(descriptor)<=2048));
CREATE TABLE IF NOT EXISTS _day_sync_members(actor TEXT PRIMARY KEY, bytes BLOB NOT NULL CHECK(length(bytes)<=4096), published INTEGER NOT NULL DEFAULT 0 CHECK(published IN (0,1)));
-- Record revisions. A transaction requiring a newer application revision than this build reads
-- is stored and ordered like any other, but its values are not projected: each object it touches
-- stays held, at its last readable value, until a build that reads it opens the store.
CREATE TABLE IF NOT EXISTS _day_sync_revisions (
 actor TEXT NOT NULL, sequence INTEGER NOT NULL, requires INTEGER NOT NULL CHECK(requires>0),
 revision INTEGER NOT NULL CHECK(revision>=requires), PRIMARY KEY(actor,sequence),
 FOREIGN KEY(actor,sequence) REFERENCES _day_sync_transactions(actor,sequence)
);
CREATE TABLE IF NOT EXISTS _day_sync_held (object TEXT NOT NULL PRIMARY KEY, requires INTEGER NOT NULL CHECK(requires>1));
