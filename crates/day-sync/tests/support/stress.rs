// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Synthetic fixtures only. The reference model does not call the engine's merge or resolver.
use super::*;
use serde::Deserialize;
use std::{io::Write, path::PathBuf};

#[derive(Deserialize)]
struct Record {
    actor: String,
    sequence: u64,
    lamport: u64,
    context: BTreeMap<String, u64>,
    changes: Vec<Change>,
}

#[derive(Deserialize)]
struct Envelope {
    transaction: Record,
}

/// Keep the original operations, databases and remote objects when an assertion fails.
/// Files are also written during the run so a process timeout leaves useful evidence.
struct Evidence {
    directory: Option<tempfile::TempDir>,
    trace: std::fs::File,
    history: std::fs::File,
    store: Faults,
    remote_directory: Vec<String>,
}

impl Evidence {
    fn new(seed: u64, steps: u64, space: &Space, store: Faults) -> Self {
        let mut builder = tempfile::Builder::new();
        let prefix = format!("day-sync-seed-{seed}-");
        builder.prefix(&prefix);
        let directory = if let Some(root) = std::env::var_os("DAY_SYNC_STRESS_ARTIFACTS") {
            std::fs::create_dir_all(&root).unwrap();
            builder.tempdir_in(root).unwrap()
        } else {
            builder.tempdir().unwrap()
        };
        let path = directory.path();
        std::fs::write(
            path.join("replay.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "seed": seed, "steps": steps, "space": space,
                "command": format!("DAY_SYNC_STRESS_SEED={seed} DAY_SYNC_STRESS_SEEDS=1 DAY_SYNC_STRESS_STEPS={steps} cargo test -p day-sync --test faults seeded_restart_and_transport_schedules_match_original_operation_replay -- --exact --nocapture")
            })).unwrap(),
        ).unwrap();
        Self {
            trace: std::fs::File::create(path.join("schedule.jsonl")).unwrap(),
            history: std::fs::File::create(path.join("operations.jsonl")).unwrap(),
            directory: Some(directory),
            store,
            remote_directory: vec![format!("day-sync-v5-{}", space.id)],
        }
    }

    fn path(&self) -> PathBuf {
        self.directory.as_ref().unwrap().path().to_owned()
    }

    fn event(&mut self, event: &serde_json::Value) {
        writeln!(self.trace, "{event}").unwrap();
        self.trace.flush().unwrap();
    }

    fn operation(&mut self, object: &Object) {
        writeln!(
            self.history,
            "{}",
            std::str::from_utf8(&object.bytes).unwrap()
        )
        .unwrap();
        self.history.flush().unwrap();
    }
}

impl Drop for Evidence {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let path = self.directory.take().unwrap().keep();
            // Never obscure the original failure with an artifact-writing panic.
            if let Ok(bytes) = serde_json::to_vec(&self.store.inner.objects(&self.remote_directory))
            {
                let _ = std::fs::write(path.join("remote.json"), bytes);
            }
            eprintln!("day-sync stress evidence: {}", path.display());
        }
    }
}

fn number(name: &str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("{name}: {error}"),
    }
}

/// A candidate survives exactly when no later observed edit to that object supersedes it.
/// This deliberately uses an O(n²) history scan, not the engine's incremental merge algorithm.
fn expected(history: &[Object]) -> (Vec<Candidate>, Vec<Bookmark>, BTreeMap<String, u64>) {
    let records: Vec<Record> = history
        .iter()
        .map(|o| {
            serde_json::from_slice::<Envelope>(&o.bytes)
                .unwrap()
                .transaction
        })
        .collect();
    let mut frontier = BTreeMap::new();
    let mut live = Vec::new();
    for record in &records {
        frontier
            .entry(record.actor.clone())
            .and_modify(|n: &mut u64| *n = (*n).max(record.sequence))
            .or_insert(record.sequence);
        for change in &record.changes {
            if !records.iter().any(|later| {
                later.context.get(&record.actor).copied().unwrap_or(0) >= record.sequence
                    && later.changes.iter().any(|c| c.id == change.id)
            }) {
                live.push((
                    Candidate {
                        id: change.id.clone(),
                        actor: record.actor.clone(),
                        sequence: record.sequence,
                        value: change.value.clone(),
                    },
                    record.lamport,
                ));
            }
        }
    }
    let mut winners = BTreeMap::new();
    for (candidate, lamport) in &live {
        let priority = (
            candidate.value.is_none(),
            *lamport,
            &candidate.actor,
            candidate.sequence,
        );
        let entry = winners
            .entry(candidate.id.clone())
            .or_insert((priority, &candidate.value));
        if priority > entry.0 {
            *entry = (priority, &candidate.value);
        }
    }
    let bookmarks = winners
        .into_iter()
        .filter_map(|(id, (_, value))| {
            value.as_ref().map(|v| Bookmark {
                id,
                url: v.url.clone(),
                title: v.title.clone(),
            })
        })
        .collect();
    let mut candidates: Vec<_> = live.into_iter().map(|(c, _)| c).collect();
    candidates.sort_by(|a, b| (&a.id, &a.actor, a.sequence).cmp(&(&b.id, &b.actor, b.sequence)));
    (candidates, bookmarks, frontier)
}

pub(super) async fn run() {
    let first = number("DAY_SYNC_STRESS_SEED", 0);
    let seeds = number("DAY_SYNC_STRESS_SEEDS", scaled(32));
    let steps = number("DAY_SYNC_STRESS_STEPS", 128);
    assert!(
        seeds > 0 && steps > 0,
        "stress seeds and steps must be positive"
    );
    for initial in first..first.checked_add(seeds).expect("seed range overflow") {
        // Fixed identities make tie-breaking and wire bytes reproducible from the seed.
        let mut space = Identity::create_space().unwrap().space;
        space.id = uuid::Uuid::from_u128(u128::from(initial) + 1).to_string();
        let store = Faults::new();
        let mut evidence = Evidence::new(initial, steps, &space, store.clone());
        let identities: Vec<_> = (101..=103)
            .map(|n| {
                Arc::new(Identity {
                    space: space.clone(),
                    actor: uuid::Uuid::from_u128(n).to_string(),
                })
            })
            .collect();
        let mut replicas = Vec::new();
        for (i, identity) in identities.iter().enumerate() {
            replicas.push(
                Replica::open(evidence.path().join(format!("r{i}")), identity.clone())
                    .await
                    .unwrap(),
            );
        }
        let objects: Vec<_> = (201..=205)
            .map(|n| uuid::Uuid::from_u128(n).to_string())
            .collect();
        let mut seed = initial.wrapping_add(1);
        let mut history = Vec::new();
        // This reader remains offline through all edits and compactions.
        let offline = Replica::open(
            evidence.path().join("offline"),
            Arc::new(Identity {
                space: space.clone(),
                actor: uuid::Uuid::from_u128(104).to_string(),
            }),
        )
        .await
        .unwrap();
        offline.sync(&store).await.unwrap();
        for step in 0..steps {
            seed = seed
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let i = ((seed >> 32) % 3) as usize;
            evidence.event(&serde_json::json!({"step": step, "replica": i, "choice": seed}));
            match seed % 6 {
                0 | 1 => {
                    replicas[i]
                        .commit(vec![edit(&objects[((seed >> 40) % 5) as usize], step)])
                        .await
                        .unwrap();
                    let object = replicas[i].local_objects().await.unwrap().pop().unwrap();
                    evidence.operation(&object);
                    history.push(object);
                }
                2 => {
                    replicas[i].close().await.unwrap();
                    replicas[i] =
                        Replica::open(evidence.path().join(format!("r{i}")), identities[i].clone())
                            .await
                            .unwrap();
                }
                _ => {
                    let op = ['l', 'g', 'c', 'p', 'd'][((seed >> 48) % 5) as usize];
                    let offset = ((seed >> 56) % 8) as usize;
                    let after = seed & (1 << 12) != 0;
                    store.arm(op, offset, after);
                    let result = if seed & 128 != 0 {
                        replicas[i].compact(&store).await
                    } else {
                        replicas[i].sync(&store).await
                    };
                    evidence.event(&serde_json::json!({"step": step, "fault": op.to_string(), "offset": offset, "after": after, "result": format!("{result:?}")}));
                    assert!(
                        result.is_ok() || matches!(result, Err(Error::Store(_))),
                        "seed={initial} step={step} op={op}: {result:?}"
                    );
                    *store.fault.lock().unwrap() = None;
                }
            }
        }
        // Recovery must work in the same process, without restarting away a stuck state.
        for _ in 0..3 {
            for r in &replicas {
                r.sync(&store).await.unwrap();
            }
        }
        replicas[0].compact(&store).await.unwrap();
        let fresh = Replica::open(
            evidence.path().join("fresh"),
            Arc::new(Identity {
                space: space.clone(),
                actor: uuid::Uuid::from_u128(105).to_string(),
            }),
        )
        .await
        .unwrap();
        fresh.sync(&store).await.unwrap();
        offline.sync(&store).await.unwrap();
        let (candidates, bookmarks, frontier) = expected(&history);
        for r in replicas.iter().chain([&fresh, &offline]) {
            let got = r.snapshot().await.unwrap();
            assert_eq!(got.candidates, candidates, "seed={initial}");
            assert_eq!(got.bookmarks, bookmarks, "seed={initial}");
            assert_eq!(got.frontier, frontier, "seed={initial}");
            assert_eq!(
                (got.pending_uploads, got.waiting, got.held, got.poisoned),
                (0, 0, 0, false),
                "seed={initial}"
            );
            r.close().await.unwrap();
        }
    }
}
