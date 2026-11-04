// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
//! Synthetic histories: this replay oracle never calls State::join.
use crate::{state::*, *};
use std::collections::BTreeMap;
#[derive(Clone)]
struct Event {
    actor: String,
    sequence: u64,
    clock: u64,
    context: BTreeMap<String, u64>,
    object: String,
    value: Option<BookmarkValue>,
    revision: u32,
    requires: u32,
    hash: String,
}
fn replay(space: &Space, events: &[Event], frontier: &BTreeMap<String, Head>) -> State {
    let mut s = State {
        space: space.clone(),
        heads: BTreeMap::new(),
        objects: BTreeMap::new(),
    };
    for e in events.iter().filter(|e| {
        frontier
            .get(&e.actor)
            .is_some_and(|h| h.sequence >= e.sequence)
    }) {
        let cs = s.objects.entry(e.object.clone()).or_default();
        cs.retain(|c| {
            c.requires > e.revision || e.context.get(&c.actor).copied().unwrap_or(0) < c.sequence
        });
        cs.push(state::Candidate {
            actor: e.actor.clone(),
            sequence: e.sequence,
            lamport: e.clock,
            requires: e.requires,
            revision: e.revision,
            value: e.value.clone(),
        });
        cs.sort_by(|a, b| (&a.actor, a.sequence).cmp(&(&b.actor, b.sequence)));
        s.heads.insert(
            e.actor.clone(),
            Head {
                sequence: e.sequence,
                hash: e.hash.clone(),
                lamport: e.clock,
            },
        );
    }
    s
}
fn random(seed: &mut u64, n: u64) -> u64 {
    *seed = seed
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    (*seed >> 32) % n
}
#[test]
fn ten_thousand_histories_match_independent_replay_and_join_laws() {
    let space = Identity::create_space().unwrap().space;
    let actors: Vec<_> = (1..=3)
        .map(|n| uuid::Uuid::from_u128(n).to_string())
        .collect();
    let objects: Vec<_> = (10..14)
        .map(|n| uuid::Uuid::from_u128(n).to_string())
        .collect();
    for initial in 0..10_000 {
        let mut seed = initial;
        let empty = State {
            space: space.clone(),
            heads: BTreeMap::new(),
            objects: BTreeMap::new(),
        };
        let mut states = vec![empty; 3];
        let mut events = vec![];
        for step in 0..60 {
            let a = random(&mut seed, 3) as usize;
            if random(&mut seed, 3) == 0 {
                let revision = 1 + random(&mut seed, 4) as u32;
                let e = Event {
                    actor: actors[a].clone(),
                    sequence: states[a].sequence(&actors[a]) + 1,
                    clock: states[a]
                        .heads
                        .values()
                        .map(|h| h.lamport)
                        .max()
                        .unwrap_or(0)
                        + 1,
                    context: states[a]
                        .heads
                        .iter()
                        .map(|(a, h)| (a.clone(), h.sequence))
                        .collect(),
                    object: objects[random(&mut seed, 4) as usize].clone(),
                    value: if random(&mut seed, 4) == 0 {
                        None
                    } else {
                        Some(BookmarkValue {
                            url: "synthetic".into(),
                            title: format!("{initial}/{step}"),
                        })
                    },
                    requires: 1 + random(&mut seed, revision.into()) as u32,
                    revision,
                    hash: wire::digest(format!("synthetic/{initial}/{step}").as_bytes()),
                };
                let mut heads = states[a].heads.clone();
                heads.insert(
                    e.actor.clone(),
                    Head {
                        sequence: e.sequence,
                        hash: e.hash.clone(),
                        lamport: e.clock,
                    },
                );
                events.push(e);
                states[a] = replay(&space, &events, &heads);
            } else {
                let b = random(&mut seed, 3) as usize;
                let joined = states[a].join(&states[b]).unwrap();
                assert_eq!(
                    joined,
                    replay(&space, &events, &joined.heads),
                    "seed {initial} step {step}"
                );
                assert_eq!(joined, states[b].join(&states[a]).unwrap());
                assert_eq!(joined, joined.join(&joined).unwrap());
                assert_eq!(
                    joined.join(&states[2]).unwrap(),
                    states[a]
                        .join(&states[b].join(&states[2]).unwrap())
                        .unwrap()
                );
                joined.validate().unwrap();
                states[a] = joined;
            }
        }
    }
}
#[test]
fn manifest_rejects_corruption_truncation_noncanonical_and_wrong_owner() {
    let space = Identity::create_space().unwrap().space;
    let s = State {
        space: space.clone(),
        heads: BTreeMap::new(),
        objects: BTreeMap::new(),
    };
    let bytes = s.encode().unwrap();
    let m = Manifest::from_body(&bytes).unwrap();
    let encoded = identity::encode(&m).unwrap();
    assert_eq!(
        Manifest::decode(&m.name(), &encoded, &space)
            .unwrap()
            .verify(&bytes)
            .unwrap(),
        s
    );
    for n in 0..encoded.len() {
        assert!(Manifest::decode(&m.name(), &encoded[..n], &space).is_err());
    }
    let mut bad = bytes.clone();
    bad.push(b' ');
    assert!(m.verify(&bad).is_err());
    assert!(Manifest::decode("snap-invalid.ds5", &encoded, &space).is_err());
    let other = Identity::create_space().unwrap().space;
    assert!(Manifest::decode(&m.name(), &encoded, &other).is_err());
    let mut bad = m.clone();
    bad.parts[0].bytes += 1;
    assert!(Manifest::decode(&bad.name(), &identity::encode(&bad).unwrap(), &space).is_err());
    let mut bad = m.clone();
    bad.bytes = usize::MAX;
    assert!(Manifest::decode(&bad.name(), &identity::encode(&bad).unwrap(), &space).is_err());
}

#[test]
fn equal_frontiers_cannot_hide_different_candidate_states() {
    let actor = uuid::Uuid::from_u128(1).to_string();
    let mut a = State {
        space: Identity::create_space().unwrap().space,
        heads: BTreeMap::from([(
            actor.clone(),
            Head {
                sequence: 1,
                lamport: 1,
                hash: wire::digest(b"synthetic"),
            },
        )]),
        objects: BTreeMap::new(),
    };
    let b = a.clone();
    a.objects.insert(
        uuid::Uuid::from_u128(2).to_string(),
        vec![state::Candidate {
            actor,
            sequence: 1,
            lamport: 1,
            requires: 1,
            revision: 1,
            value: None,
        }],
    );
    a.validate().unwrap();
    b.validate().unwrap();
    assert!(matches!(a.join(&b), Err(Error::Integrity)));
    assert!(matches!(b.join(&a), Err(Error::Integrity)));
}

#[test]
fn genesis_has_a_fixed_domain_separated_wire_vector() {
    // Independently calculated with Python's hashlib.sha256 over these exact UTF-8 bytes.
    let bytes = br#"{"kind":"day-sync-v5-genesis","space":{"version":5,"encoding":"plain-json-v1","id":"00000000-0000-0000-0000-000000000001","profile":"day-sync:bookmark:5"},"actor":"00000000-0000-0000-0000-000000000002","revision":1,"changes":[[{"id":"00000000-0000-0000-0000-000000000003","value":null},1]]}"#;
    let expected = "3674d61b9e173c00662e8f251dc782504b40e7391d3cc5ca77f68547edafc4a5";
    let g = genesis::Genesis::decode(bytes).unwrap();
    assert_eq!(identity::encode(&g).unwrap(), bytes);
    let m = Manifest::from_genesis(bytes).unwrap();
    assert_eq!(m.state, expected);
    let s = m.verify(bytes).unwrap();
    assert_eq!(s.heads[&g.actor].hash, expected);
    assert_eq!(s.heads[&g.actor].sequence, 1);
    assert_eq!(s.objects.values().next().unwrap()[0].value, None);
    assert!(State::decode(bytes).is_err());
    let mut bad = bytes.to_vec();
    bad.push(b' ');
    assert!(genesis::Genesis::decode(&bad).is_err());
}

#[test]
fn snapshot_rejects_impossible_same_actor_candidate_clocks() {
    let actor = uuid::Uuid::from_u128(1).to_string();
    let mut s = State {
        space: Identity::create_space().unwrap().space,
        heads: BTreeMap::from([(
            actor.clone(),
            Head {
                sequence: 3,
                lamport: 3,
                hash: wire::digest(b"synthetic"),
            },
        )]),
        objects: BTreeMap::from([(
            uuid::Uuid::from_u128(2).to_string(),
            vec![state::Candidate {
                actor: actor.clone(),
                sequence: 1,
                lamport: 3,
                requires: 2,
                revision: 2,
                value: None,
            }],
        )]),
    };
    assert!(s.validate().is_err());
    let candidates = s.objects.values_mut().next().unwrap();
    candidates[0].lamport = 2;
    candidates.push(state::Candidate {
        actor,
        sequence: 2,
        lamport: 2,
        requires: 1,
        revision: 1,
        value: None,
    });
    assert!(s.validate().is_err());
    s.objects.values_mut().next().unwrap()[0].lamport = 1;
    s.validate().unwrap();
}

#[test]
fn dominated_snapshot_cannot_remove_a_newer_states_live_candidate_in_either_order() {
    let actor = uuid::Uuid::from_u128(1).to_string();
    let object = uuid::Uuid::from_u128(2).to_string();
    let older = State {
        space: Identity::create_space().unwrap().space,
        heads: BTreeMap::from([(
            actor.clone(),
            Head {
                sequence: 1,
                lamport: 1,
                hash: wire::digest(b"one"),
            },
        )]),
        objects: BTreeMap::new(),
    };
    let mut newer = older.clone();
    newer.heads.insert(
        actor.clone(),
        Head {
            sequence: 2,
            lamport: 2,
            hash: wire::digest(b"two"),
        },
    );
    newer.objects.insert(
        object,
        vec![state::Candidate {
            actor,
            sequence: 1,
            lamport: 1,
            requires: 1,
            revision: 1,
            value: None,
        }],
    );
    older.validate().unwrap();
    newer.validate().unwrap();
    assert!(matches!(newer.join(&older), Err(Error::Integrity)));
    assert!(matches!(older.join(&newer), Err(Error::Integrity)));
}

#[test]
fn candidates_of_one_event_must_agree_across_objects() {
    let actor = uuid::Uuid::from_u128(1).to_string();
    let c = state::Candidate {
        actor: actor.clone(),
        sequence: 1,
        lamport: 1,
        requires: 1,
        revision: 2,
        value: None,
    };
    let mut s = State {
        space: Identity::create_space().unwrap().space,
        heads: BTreeMap::from([(
            actor,
            Head {
                sequence: 3,
                lamport: 4,
                hash: wire::digest(b"synthetic"),
            },
        )]),
        objects: BTreeMap::from([
            (uuid::Uuid::from_u128(2).to_string(), vec![c.clone()]),
            (uuid::Uuid::from_u128(3).to_string(), vec![c]),
        ]),
    };
    s.validate().unwrap();
    let key = uuid::Uuid::from_u128(3).to_string();
    s.objects.get_mut(&key).unwrap()[0].revision = 3;
    assert!(matches!(s.validate(), Err(Error::InvalidWire)));
    s.objects.get_mut(&key).unwrap()[0].revision = 2;
    s.objects.get_mut(&key).unwrap()[0].lamport = 2;
    assert!(matches!(s.validate(), Err(Error::InvalidWire)));
    s.objects.get_mut(&key).unwrap()[0].lamport = 1;
    s.objects.get_mut(&key).unwrap()[0].sequence = 2;
    s.objects.get_mut(&key).unwrap()[0].lamport = 2;
    s.objects.values_mut().next().unwrap()[0].lamport = 3; // reverse clocks across objects
    assert!(matches!(s.validate(), Err(Error::InvalidWire)));
}

#[test]
fn newer_snapshots_cannot_forget_known_objects_including_tombstones() {
    let actor = uuid::Uuid::from_u128(1).to_string();
    let object = uuid::Uuid::from_u128(2).to_string();
    let other = uuid::Uuid::from_u128(3).to_string();
    for value in [
        None,
        Some(BookmarkValue {
            url: "synthetic://retention".into(),
            title: "known object".into(),
        }),
    ] {
        let candidate = state::Candidate {
            actor: actor.clone(),
            sequence: 1,
            lamport: 1,
            requires: 1,
            revision: 1,
            value,
        };
        let older = State {
            space: Identity::create_space().unwrap().space,
            heads: BTreeMap::from([(
                actor.clone(),
                Head {
                    sequence: 1,
                    lamport: 1,
                    hash: wire::digest(b"one"),
                },
            )]),
            objects: BTreeMap::from([(object.clone(), vec![candidate])]),
        };
        let mut newer = older.clone();
        newer.heads.get_mut(&actor).unwrap().sequence = 2;
        newer.heads.get_mut(&actor).unwrap().lamport = 2;
        newer.heads.get_mut(&actor).unwrap().hash = wire::digest(b"two");
        newer.objects.clear();
        let deletion = state::Candidate {
            actor: actor.clone(),
            sequence: 2,
            lamport: 2,
            requires: 1,
            revision: 1,
            value: None,
        };
        newer.objects.insert(other.clone(), vec![deletion.clone()]);
        newer.validate().unwrap();
        assert!(matches!(older.join(&newer), Err(Error::Integrity)));
        assert!(matches!(newer.join(&older), Err(Error::Integrity)));
        newer.objects.insert(object.clone(), vec![deletion]);
        assert_eq!(older.join(&newer).unwrap(), newer); // a real delete keeps its candidate
    }
}

#[test]
fn snapshot_encoding_preserves_the_fixed_wire_vector_and_validates_local_state() {
    // Synthetic tombstone state; digest independently calculated with Python hashlib.sha256.
    let bytes = br#"{"space":{"version":5,"encoding":"plain-json-v1","id":"00000000-0000-0000-0000-000000000001","profile":"day-sync:bookmark:5"},"heads":{"00000000-0000-0000-0000-000000000002":{"sequence":1,"hash":"1111111111111111111111111111111111111111111111111111111111111111","lamport":1}},"objects":{"00000000-0000-0000-0000-000000000003":[{"actor":"00000000-0000-0000-0000-000000000002","sequence":1,"lamport":1,"requires":1,"revision":1,"value":null}]}}"#;
    let expected = "1b70151358a6a8c333321a5b12444108a77876355d83d15297fd8b5c23f183cb";
    let mut state = State::decode(bytes).unwrap();
    let (encoded, manifest) = Manifest::encode_state(&state).unwrap();
    assert_eq!(encoded, bytes);
    assert_eq!(manifest.state, expected);
    assert_eq!(manifest.bytes, bytes.len());
    assert_eq!(manifest.parts.len(), 1);
    assert_eq!(manifest.parts[0].hash, expected);
    assert_eq!(manifest, Manifest::from_body(bytes).unwrap());
    assert_eq!(manifest.verify(&encoded).unwrap(), state);
    state.heads.values_mut().next().unwrap().lamport = 0;
    assert!(matches!(
        Manifest::encode_state(&state),
        Err(Error::InvalidWire)
    ));
}
