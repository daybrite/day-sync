// SPDX-License-Identifier: MPL-2.0
//! Immutable file transfer alongside a sync space. Metadata snapshots never enumerate this
//! collection. Removing a reference or evicting a local copy never deletes remote objects.
use crate::{Error, Store, store::ObjectRead, wire};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};

pub const CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_CHUNKS: usize = 65_536;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetRef {
    pub sha256: String,
    pub bytes: u64,
    /// Large chunk-pool assets reference an ordered manifest.
    pub manifest: Option<String>,
    /// A whole file in an application-owned collection. Older references use the chunk pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<AssetFile>,
}

/// The decoded directory and filename of a browsable, immutable asset.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssetFile {
    pub directory: String,
    pub name: String,
}

/// A decoded, visible filename that is a single component on native filesystems and `WebDAV`.
pub fn valid_file_component(name: &str) -> bool {
    let first = name
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end()
        .to_ascii_uppercase();
    let device = matches!(
        first.as_str(),
        "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
    ) || first
        .strip_prefix("COM")
        .or_else(|| first.strip_prefix("LPT"))
        .is_some_and(|n| {
            matches!(
                n,
                "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
            )
        });
    !device
        && !name.is_empty()
        && name.len() <= 240
        && !name.starts_with(['.', ' '])
        && !name.ends_with(['.', ' '])
        && !name
            .chars()
            .any(|c| c.is_control() || "/\\:*?\"<>|".contains(c))
}

impl AssetFile {
    /// Validate both path components before passing them to a store.
    pub fn valid(&self) -> bool {
        valid_file_component(&self.directory) && valid_file_component(&self.name)
    }
}

impl AssetRef {
    /// # Errors
    /// Reject malformed references before constructing a path or reserving space.
    pub fn validate(&self) -> Result<(), Error> {
        if !wire::valid_hash(&self.sha256)
            || self.bytes > CHUNK_BYTES as u64 * MAX_CHUNKS as u64
            || self.file.as_ref().is_some_and(|f| !f.valid())
            || self.manifest.is_some() != (self.file.is_none() && self.bytes > CHUNK_BYTES as u64)
            || self.manifest.as_ref().is_some_and(|h| !wire::valid_hash(h))
        {
            return Err(Error::InvalidWire);
        }
        Ok(())
    }
}

fn directory(space: &str, kind: &str, hash: &str) -> Result<Vec<String>, Error> {
    if !wire::valid_id(space) || !wire::valid_hash(hash) {
        return Err(Error::InvalidWire);
    }
    Ok(vec![
        format!("day-assets-v1-{space}"),
        kind.into(),
        hash[..2].into(),
    ])
}

/// A per-upload journal. Keep it through interruption, then discard after committing
/// the attachment reference. Receipts bind the endpoint, space, object path and hash.
pub struct UploadJournal(rusqlite::Connection);
impl UploadJournal {
    /// # Errors
    /// Returns an error if the on-device journal cannot be opened or initialized.
    pub fn open(path: &std::path::Path) -> Result<Self, Error> {
        let db = rusqlite::Connection::open(path).map_err(journal_error)?;
        db.execute_batch("CREATE TABLE IF NOT EXISTS receipts(endpoint TEXT,space TEXT,kind TEXT,hash TEXT,etag TEXT NOT NULL, PRIMARY KEY(endpoint,space,kind,hash)) WITHOUT ROWID").map_err(journal_error)?;
        Ok(Self(db))
    }
}

fn journal_error(e: rusqlite::Error) -> Error {
    crate::StoreError::with_source(crate::StoreErrorKind::Other, e).into()
}

async fn publish(
    store: &dyn Store,
    space: &str,
    kind: &str,
    bytes: Vec<u8>,
    journal: Option<&UploadJournal>,
) -> Result<String, Error> {
    use rusqlite::OptionalExtension as _;
    let hash = wire::digest(&bytes);
    let dir = directory(space, kind, &hash)?;
    let endpoint = store.endpoint();
    let key = [endpoint.as_str(), space, kind, hash.as_str()];
    let tag: Option<String> = match journal {
        Some(j) => {
            j.0.query_row(
                "SELECT etag FROM receipts WHERE endpoint=? AND space=? AND kind=? AND hash=?",
                key,
                |r| r.get(0),
            )
            .optional()
            .map_err(journal_error)?
        }
        None => None,
    };
    let existing = match store.read_object(&dir, &hash, tag.as_deref()).await {
        Err(e) if e.kind == crate::StoreErrorKind::NotFound => ObjectRead::Absent,
        other => other?,
    };
    let result = match existing {
        ObjectRead::Absent => {
            store.ensure_directory(&dir).await?;
            let creation = store.create(&dir, &hash, bytes).await;
            let result = store.read_object(&dir, &hash, None).await?;
            if matches!(result, ObjectRead::Absent) {
                creation?;
            }
            result
        }
        result => result,
    };
    match result {
        ObjectRead::Unchanged if tag.is_some() => Ok(hash),
        ObjectRead::Bytes(body, current) if wire::digest(&body) == hash => {
            if let (Some(j), Some(current)) = (journal, current) {
                j.0.execute(
                    "INSERT OR REPLACE INTO receipts VALUES(?,?,?,?,?)",
                    [key[0], key[1], key[2], key[3], current.as_str()],
                )
                .map_err(journal_error)?;
            }
            Ok(hash)
        }
        _ => Err(Error::Integrity),
    }
}

async fn fetch(store: &dyn Store, space: &str, kind: &str, hash: &str) -> Result<Vec<u8>, Error> {
    let bytes = store
        .get(&directory(space, kind, hash)?, hash)
        .await?
        .ok_or_else(|| crate::StoreError::new(crate::StoreErrorKind::NotFound))?;
    if wire::digest(&bytes) != hash {
        return Err(Error::Integrity);
    }
    Ok(bytes)
}

fn file_error(e: std::io::Error) -> Error {
    crate::StoreError::local_io(e).into()
}

/// Read at most one chunk at a time. Repeating an interrupted upload reuses verified objects.
/// # Errors
/// Returns local I/O, store, size, or integrity errors. Only a successful result may be published.
pub async fn upload(store: &dyn Store, space: &str, input: impl Read) -> Result<AssetRef, Error> {
    upload_with_journal(store, space, input, None).await
}

/// Upload using persistent strong-validator receipts. Servers without strong validators
/// still verify each existing chunk by reading it back.
/// # Errors
/// Returns I/O, journal, transport and integrity failures without publishing a reference.
pub async fn upload_with_journal(
    store: &dyn Store,
    space: &str,
    input: impl Read,
    journal: Option<&UploadJournal>,
) -> Result<AssetRef, Error> {
    upload_with_progress(store, space, input, journal, |_| {}).await
}

/// Upload with cumulative verified-byte progress, including reusable chunks. The callback
/// runs on the polling task, once after each verified chunk and after the manifest is ready.
/// This reports durable content progress, not bytes queued to the network.
/// # Errors
/// Returns the same I/O, journal, transport and integrity failures as [`upload_with_journal`].
pub async fn upload_with_progress(
    store: &dyn Store,
    space: &str,
    mut input: impl Read,
    journal: Option<&UploadJournal>,
    mut progress: impl FnMut(u64),
) -> Result<AssetRef, Error> {
    progress(0);
    let mut hashes = Vec::new();
    let mut whole = Sha256::new();
    let mut bytes = 0;
    loop {
        let mut chunk = Vec::new();
        input
            .by_ref()
            .take(CHUNK_BYTES as u64)
            .read_to_end(&mut chunk)
            .map_err(file_error)?;
        if chunk.is_empty() && !hashes.is_empty() {
            break;
        }
        if hashes.len() >= MAX_CHUNKS {
            return Err(Error::Limit);
        }
        bytes += chunk.len() as u64;
        whole.update(&chunk);
        let last = chunk.len() < CHUNK_BYTES;
        hashes.push(publish(store, space, "objects", chunk, journal).await?);
        progress(bytes);
        if last {
            break;
        }
    }
    let manifest = if hashes.len() > 1 {
        Some(
            publish(
                store,
                space,
                "manifests",
                serde_json::to_vec(&hashes).map_err(|_| Error::InvalidWire)?,
                journal,
            )
            .await?,
        )
    } else {
        None
    };
    progress(bytes);
    Ok(AssetRef {
        sha256: wire::hex_digest(&whole.finalize()),
        bytes,
        manifest,
        file: None,
    })
}

async fn chunk_hashes(
    store: &dyn Store,
    space: &str,
    asset: &AssetRef,
) -> Result<Vec<String>, Error> {
    asset.validate()?;
    if let Some(hash) = &asset.manifest {
        let manifest = fetch(store, space, "manifests", hash).await?;
        if manifest.len() > MAX_CHUNKS * 67 + 1 {
            return Err(Error::Limit);
        }
        let hashes: Vec<String> =
            serde_json::from_slice(&manifest).map_err(|_| Error::InvalidWire)?;
        if hashes.len() as u64 != asset.bytes.div_ceil(CHUNK_BYTES as u64)
            || hashes.iter().any(|h| !wire::valid_hash(h))
        {
            return Err(Error::InvalidWire);
        }
        Ok(hashes)
    } else {
        Ok(vec![asset.sha256.clone()])
    }
}

struct VerifiedWriter<'a, F> {
    sink: &'a mut dyn Write,
    hash: Sha256,
    count: u64,
    progress: F,
}

impl<F: FnMut(u64)> Write for VerifiedWriter<'_, F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let count = self.sink.write(bytes)?;
        self.hash.update(&bytes[..count]);
        self.count += count as u64;
        (self.progress)(self.count);
        Ok(count)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.sink.flush()
    }
}

/// Read and verify a named asset without buffering it. None denotes an absent file.
async fn read_named(
    store: &dyn Store,
    file: &AssetFile,
    sink: &mut dyn Write,
    limit: u64,
    progress: impl FnMut(u64),
) -> Result<Option<(String, u64)>, Error> {
    let mut writer = VerifiedWriter {
        sink,
        hash: Sha256::new(),
        count: 0,
        progress,
    };
    let exists = store
        .read_file(
            std::slice::from_ref(&file.directory),
            &file.name,
            &mut writer,
            limit,
        )
        .await?;
    Ok(exists.then(|| (wire::hex_digest(&writer.hash.finalize()), writer.count)))
}

/// An absent file, an exact match, or an occupied name with different bytes.
async fn named_matches(
    store: &dyn Store,
    file: &AssetFile,
    reference: &AssetRef,
) -> Result<Option<bool>, Error> {
    match read_named(store, file, &mut std::io::sink(), reference.bytes, |_| {}).await {
        Ok(result) => {
            Ok(result.map(|(hash, count)| hash == reference.sha256 && count == reference.bytes))
        }
        Err(Error::Store(e)) if e.kind == crate::StoreErrorKind::Limit => Ok(Some(false)),
        Err(Error::Store(e)) if e.kind == crate::StoreErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Publish a local file under a readable name. Verify the complete remote bytes even after
/// an ambiguous write. Keep the source immutable for the duration of the call. Returns
/// `None` when a different file already owns the name. Progress counts outgoing bytes on the
/// polling task. A verified existing copy reports its full size without another upload.
/// # Errors
/// Returns a size, integrity, transport or I/O error if publication could not be verified.
pub async fn upload_file(
    store: &dyn Store,
    file: AssetFile,
    source: &std::path::Path,
    digest: &str,
    mut progress: impl FnMut(u64),
) -> Result<Option<AssetRef>, Error> {
    let bytes = std::fs::metadata(source).map_err(file_error)?.len();
    let reference = AssetRef {
        bytes,
        sha256: digest.into(),
        manifest: None,
        file: Some(file.clone()),
    };
    reference.validate()?;
    let source_file = std::fs::File::open(source).map_err(file_error)?;
    let mut local = VerifiedWriter {
        sink: &mut std::io::sink(),
        hash: Sha256::new(),
        count: 0,
        progress: |_| {},
    };
    std::io::copy(&mut source_file.take(bytes + 1), &mut local).map_err(file_error)?;
    if local.count != bytes || wire::hex_digest(&local.hash.finalize()) != digest {
        return Err(Error::Integrity);
    }
    progress(0);
    let dir = std::slice::from_ref(&file.directory);
    match named_matches(store, &file, &reference).await? {
        Some(true) => {
            progress(bytes);
            return Ok(Some(reference));
        }
        Some(false) => return Ok(None),
        None => {}
    }
    store.ensure_directory(dir).await?;
    let mut reported = 0;
    let mut report = |n: u64| {
        let n = n.min(bytes);
        if n > reported {
            reported = n;
            progress(n);
        }
    };
    let created = store
        .create_file_with_progress(dir, &file.name, source, bytes, &mut report)
        .await;
    if let Ok(crate::store::FileCreation::Verified {
        sha256,
        bytes: count,
    }) = &created
    {
        return if wire::hex_digest(sha256) == digest && *count == bytes {
            report(bytes);
            Ok(Some(reference))
        } else {
            Err(Error::Integrity)
        };
    }
    // A racing creator or a lost response can still leave the requested bytes in place.
    match named_matches(store, &file, &reference).await {
        Ok(Some(true)) => {
            report(bytes);
            Ok(Some(reference))
        }
        Ok(Some(false)) if matches!(created, Ok(crate::store::FileCreation::Exists)) => Ok(None),
        verified => {
            created?;
            verified?;
            Err(Error::Integrity)
        }
    }
}

/// Download a named file in full or resume a chunked file from its verified prefix. The caller owns staging and
/// installs the file atomically only after success. A bad prefix is repaired on retry.
/// # Errors
/// Returns local I/O, store or integrity errors. A failed transfer is never ready to open.
pub async fn download(
    store: &dyn Store,
    space: &str,
    asset: &AssetRef,
    output: &mut std::fs::File,
) -> Result<(), Error> {
    download_with_progress(store, space, asset, output, |_| {}).await
}

/// Report named-file bytes as received and chunked bytes as verified, including any verified
/// local prefix. The callback runs on the polling task. Await success before using the file
/// so that the final integrity check and durable write have completed.
/// # Errors
/// Returns the same validation, I/O, transport and integrity failures as [`download`].
pub async fn download_with_progress(
    store: &dyn Store,
    space: &str,
    asset: &AssetRef,
    output: &mut std::fs::File,
    mut progress: impl FnMut(u64),
) -> Result<(), Error> {
    progress(0);
    asset.validate()?;
    if let Some(file) = &asset.file {
        output.rewind().map_err(file_error)?;
        let actual = read_named(store, file, output, asset.bytes, progress)
            .await?
            .ok_or_else(|| crate::StoreError::new(crate::StoreErrorKind::NotFound))?;
        if actual.0 != asset.sha256 || actual.1 != asset.bytes {
            return Err(Error::Integrity);
        }
        output.set_len(asset.bytes).map_err(file_error)?;
        return output.sync_all().map_err(file_error);
    }
    let hashes = chunk_hashes(store, space, asset).await?;
    let mut offset = 0;
    let mut whole = Sha256::new();
    for hash in hashes {
        let count = (asset.bytes - offset).min(CHUNK_BYTES as u64);
        let mut chunk = Vec::new();
        output.seek(SeekFrom::Start(offset)).map_err(file_error)?;
        Read::by_ref(output)
            .take(count)
            .read_to_end(&mut chunk)
            .map_err(file_error)?;
        if chunk.len() as u64 != count || wire::digest(&chunk) != hash {
            chunk = fetch(store, space, "objects", &hash).await?;
            if chunk.len() as u64 != count {
                return Err(Error::Integrity);
            }
            output.seek(SeekFrom::Start(offset)).map_err(file_error)?;
            output.write_all(&chunk).map_err(file_error)?;
        }
        whole.update(&chunk);
        offset += count;
        progress(offset);
    }
    if wire::hex_digest(&whole.finalize()) != asset.sha256 {
        return Err(Error::Integrity);
    }
    output.set_len(asset.bytes).map_err(file_error)?;
    output.sync_all().map_err(file_error)
}

/// Restore one attachment from a verified backup into a new library, using bounded
/// memory. Named files stage in the caller’s writable directory during transfer.
///
/// # Errors
/// Refuses an invalid reference, a missing chunk, or a differing publication.
pub async fn copy(
    source: &dyn Store,
    from: &str,
    target: &dyn Store,
    to: &str,
    reference: &AssetRef,
    staging_directory: &std::path::Path,
) -> Result<(), Error> {
    reference.validate()?;
    if let Some(file) = &reference.file {
        let mut temp = tempfile::NamedTempFile::new_in(staging_directory).map_err(file_error)?;
        download(source, from, reference, temp.as_file_mut()).await?;
        upload_file(target, file.clone(), temp.path(), &reference.sha256, |_| {})
            .await?
            .ok_or(Error::Integrity)?;
        return Ok(());
    }
    let hashes = chunk_hashes(source, from, reference).await?;
    let mut offset = 0;
    let mut whole = Sha256::new();
    for hash in &hashes {
        let chunk = fetch(source, from, "objects", hash).await?;
        let expected = (reference.bytes - offset).min(CHUNK_BYTES as u64);
        if chunk.len() as u64 != expected {
            return Err(Error::Integrity);
        }
        whole.update(&chunk);
        publish(target, to, "objects", chunk, None).await?;
        offset += expected;
    }
    if wire::hex_digest(&whole.finalize()) != reference.sha256 {
        return Err(Error::Integrity);
    }
    if let Some(hash) = &reference.manifest {
        let manifest = fetch(source, from, "manifests", hash).await?;
        publish(target, to, "manifests", manifest, None).await?;
    }
    Ok(())
}

pub(crate) async fn backup_pool(
    source: &dyn Store,
    target: &dyn Store,
    space: &str,
) -> Result<Vec<(Vec<String>, String)>, Error> {
    let mut inventory = Vec::new();
    for kind in ["objects", "manifests"] {
        let base = [format!("day-assets-v1-{space}"), kind.into()];
        let shards = match source.list(&base).await {
            Err(e) if e.kind == crate::StoreErrorKind::NotFound => continue,
            other => other?,
        };
        for shard in shards {
            let prefix = shard.trim_end_matches('/');
            if prefix.len() != 2
                || !prefix
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                continue;
            }
            let mut dir = base.to_vec();
            dir.push(prefix.into());
            for name in source.list(&dir).await? {
                if !wire::valid_hash(&name) || !name.starts_with(prefix) {
                    continue;
                }
                let body = fetch(source, space, kind, &name).await?;
                if publish(target, space, kind, body, None).await? != name {
                    return Err(Error::Integrity);
                }
                inventory.push((dir.clone(), name));
            }
        }
    }
    Ok(inventory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Identity, MemoryStore};
    use futures_lite::future::block_on;
    use std::io::Cursor;
    #[test]
    fn named_and_chunked_wire_references_remain_distinct() {
        let hash = wire::digest(b"fixture");
        let legacy = serde_json::json!({"sha256":hash,"bytes":7,"manifest":null});
        let chunk: AssetRef = serde_json::from_value(legacy.clone()).unwrap();
        assert!(chunk.file.is_none());
        assert_eq!(serde_json::to_value(chunk).unwrap(), legacy);
        let mut named = legacy;
        named["file"] = serde_json::json!({"directory":"books","name":"Fixture.epub"});
        let file: AssetRef = serde_json::from_value(named.clone()).unwrap();
        file.validate().unwrap();
        assert_eq!(serde_json::to_value(file).unwrap(), named);
        named["future_storage"] = serde_json::json!(2);
        assert!(serde_json::from_value::<AssetRef>(named).is_err());
    }
    #[test]
    fn named_assets_validate_the_source_and_preserve_occupied_files() {
        block_on(async {
            let scratch = tempfile::tempdir().unwrap();
            let store = MemoryStore::new();
            let file = AssetFile {
                directory: "books".into(),
                name: "Fixture edition.epub".into(),
            };
            let dir = std::slice::from_ref(&file.directory);
            let mut local = tempfile::NamedTempFile::new().unwrap();
            local.write_all(b"synthetic edition").unwrap();
            let hash = wire::digest(b"synthetic edition");
            let wrong = wire::digest(b"other bytes");
            assert!(matches!(
                upload_file(&store, file.clone(), local.path(), &wrong, |_| {}).await,
                Err(Error::Integrity)
            ));
            assert!(store.get(dir, &file.name).await.unwrap().is_none());
            let reference = upload_file(&store, file.clone(), local.path(), &hash, |_| {})
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                upload_file(&store, file.clone(), local.path(), &hash, |_| {})
                    .await
                    .unwrap(),
                Some(reference.clone())
            );
            let mut other = tempfile::NamedTempFile::new().unwrap();
            other.write_all(b"other bytes").unwrap();
            assert!(
                upload_file(&store, file.clone(), other.path(), &wrong, |_| {})
                    .await
                    .unwrap()
                    .is_none()
            );
            let backup = MemoryStore::new();
            let space = Identity::create_space().unwrap();
            copy(
                &store,
                space.space(),
                &backup,
                space.space(),
                &reference,
                scratch.path(),
            )
            .await
            .unwrap();
            assert_eq!(
                backup.get(dir, &file.name).await.unwrap().unwrap(),
                b"synthetic edition"
            );
            assert!(store.corrupt(dir, &file.name, b"corrupted".to_vec()));
            let mut output = tempfile::tempfile().unwrap();
            assert!(matches!(
                download(&store, space.space(), &reference, &mut output).await,
                Err(Error::Integrity)
            ));
        });
    }
    #[test]
    fn progress_counts_verified_chunks_and_reused_prefixes_but_not_corrupt_data() {
        block_on(async {
            let store = MemoryStore::new();
            let identity = Identity::create_space().unwrap();
            let space = identity.space();
            let bytes: Vec<u8> = (0..CHUNK_BYTES + 137).map(|i| (i % 251) as u8).collect();
            let mut uploaded = Vec::new();
            let asset =
                upload_with_progress(&store, space, bytes.as_slice(), None, |n| uploaded.push(n))
                    .await
                    .unwrap();
            assert_eq!(uploaded.first(), Some(&0));
            assert_eq!(uploaded.last(), Some(&(bytes.len() as u64)));
            assert!(uploaded.windows(2).all(|pair| pair[0] <= pair[1]));
            assert!(uploaded.contains(&(CHUNK_BYTES as u64)));
            let hashes = chunk_hashes(&store, space, &asset).await.unwrap();
            let mut output = tempfile::tempfile().unwrap();
            output.write_all(&bytes[..CHUNK_BYTES]).unwrap();
            // The verified local prefix is sufficient even when the remote first chunk is gone.
            assert!(store.lose(
                &directory(space, "objects", &hashes[0]).unwrap(),
                &hashes[0]
            ));
            let mut downloaded = Vec::new();
            download_with_progress(&store, space, &asset, &mut output, |n| downloaded.push(n))
                .await
                .unwrap();
            assert_eq!(downloaded, vec![0, CHUNK_BYTES as u64, bytes.len() as u64]);
            assert!(store.corrupt(
                &directory(space, "objects", &hashes[1]).unwrap(),
                &hashes[1],
                vec![9]
            ));
            output.set_len(CHUNK_BYTES as u64).unwrap();
            downloaded.clear();
            assert!(
                download_with_progress(&store, space, &asset, &mut output, |n| downloaded.push(n))
                    .await
                    .is_err()
            );
            assert_eq!(downloaded, vec![0, CHUNK_BYTES as u64]);
        });
    }

    #[test]
    #[ignore = "streams a synthetic file larger than 4 GiB"]
    fn media_offsets_cross_four_gibibytes() {
        block_on(async {
            let store = MemoryStore::new();
            let identity = Identity::create_space().unwrap();
            let space = identity.space();
            let boundary = 1_u64 << 32;
            let source = std::io::repeat(0).take(boundary).chain(Cursor::new([42]));
            let reference = upload(&store, space, source).await.unwrap();
            assert_eq!(reference.bytes, boundary + 1);
            // A sparse staging file provides the verified prefix without reserving 4 GiB.
            let mut staging = tempfile::tempfile().unwrap();
            staging.set_len(boundary).unwrap();
            download(&store, space, &reference, &mut staging)
                .await
                .unwrap();
            assert_eq!(staging.metadata().unwrap().len(), boundary + 1);
            staging.seek(SeekFrom::Start(boundary - 1)).unwrap();
            let mut tail = [0; 2];
            staging.read_exact(&mut tail).unwrap();
            assert_eq!(tail, [0, 42]);
        });
    }

    #[test]
    fn empty_small_boundary_and_resumed_large_objects() {
        block_on(async {
            let scratch = tempfile::tempdir().unwrap();
            let store = MemoryStore::new();
            let space = Identity::create_space().unwrap();
            let bytes: Vec<u8> = (0..(2 * CHUNK_BYTES + 11))
                .map(|i| (i % 251) as u8)
                .collect();
            for size in [0, 13, CHUNK_BYTES, CHUNK_BYTES + 1, bytes.len()] {
                let a = upload(&store, space.space(), Cursor::new(&bytes[..size]))
                    .await
                    .unwrap();
                assert_eq!(a.manifest.is_some(), size > CHUNK_BYTES);
                assert_eq!(a.sha256, wire::digest(&bytes[..size]));
                let mut f = tempfile::tempfile().unwrap();
                f.write_all(&bytes[..size / 2]).unwrap();
                download(&store, space.space(), &a, &mut f).await.unwrap();
                f.rewind().unwrap();
                let mut received = Vec::new();
                f.read_to_end(&mut received).unwrap();
                assert_eq!(received, bytes[..size]);
            }
            let a = upload(&store, space.space(), Cursor::new(&bytes))
                .await
                .unwrap();
            let target = MemoryStore::new();
            let other = Identity::create_space().unwrap();
            copy(
                &store,
                space.space(),
                &target,
                other.space(),
                &a,
                scratch.path(),
            )
            .await
            .unwrap();
            let mut restored = tempfile::tempfile().unwrap();
            download(&target, other.space(), &a, &mut restored)
                .await
                .unwrap();
            assert_eq!(restored.metadata().unwrap().len(), bytes.len() as u64);
            let hash = wire::digest(&bytes[CHUNK_BYTES..2 * CHUNK_BYTES]);
            let dir = directory(space.space(), "objects", &hash).unwrap();
            store.corrupt(&dir, &hash, vec![42]);
            assert!(matches!(
                copy(
                    &store,
                    space.space(),
                    &target,
                    other.space(),
                    &a,
                    scratch.path()
                )
                .await,
                Err(Error::Integrity)
            ));
            let mut f = tempfile::tempfile().unwrap();
            assert!(matches!(
                download(&store, space.space(), &a, &mut f).await,
                Err(Error::Integrity)
            ));
            assert_eq!(f.metadata().unwrap().len(), CHUNK_BYTES as u64);
            store
                .put(&dir, &hash, bytes[CHUNK_BYTES..2 * CHUNK_BYTES].to_vec())
                .await
                .unwrap();
            // The verified first chunk can resume even if the server temporarily loses it.
            let first = wire::digest(&bytes[..CHUNK_BYTES]);
            store.lose(
                &directory(space.space(), "objects", &first).unwrap(),
                &first,
            );
            download(&store, space.space(), &a, &mut f).await.unwrap();
            f.rewind().unwrap();
            let mut received = Vec::new();
            f.read_to_end(&mut received).unwrap();
            assert_eq!(received, bytes);
            let mut corrupt = a;
            corrupt.sha256 = wire::digest(b"synthetic mismatch");
            assert!(matches!(
                download(&store, space.space(), &corrupt, &mut f).await,
                Err(Error::Integrity)
            ));
        });
    }
}
