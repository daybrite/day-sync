// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
use super::*;
#[test]
fn format_boundary_rejects_unknown_protection_and_old_encrypted_format() {
    let id = Identity::create_space().unwrap();
    let mut s = id.space.clone();
    s.encoding = "future-encrypted".into();
    assert!(matches!(s.validate(), Err(Error::UnsupportedFormat)));
    assert!(Identity::from_bytes(b"DAYENC02encrypted").is_err());
    let tx = wire::Transaction {
        version: 5,
        requires: 1,
        revision: 1,
        space: id.space().into(),
        actor: id.actor().into(),
        sequence: 1,
        lamport: 1,
        previous: None,
        context: Default::default(),
        changes: vec![Change {
            id: uuid::Uuid::new_v4().to_string(),
            value: Some(BookmarkValue {
                url: "https://fixture.invalid".into(),
                title: "VISIBLE PLAINTEXT".into(),
            }),
        }],
    };
    let bytes = id.encode_record(&tx).unwrap();
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("VISIBLE PLAINTEXT")
    );
    assert_eq!(id.decode_record(&bytes).unwrap(), tx);
    let mut other = id.clone();
    other.space.profile = "different-app".into();
    assert!(matches!(
        other.decode_record(&bytes),
        Err(Error::WrongSpace)
    ));
    for n in 0..bytes.len() {
        assert!(id.decode_record(&bytes[..n]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(b' ');
    assert!(id.decode_record(&trailing).is_err());
    let mut malformed = tx.clone();
    malformed.version = 2;
    assert!(id.encode_record(&malformed).is_err());
    // First-revision records keep the original bytes; later revisions name both numbers.
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(!text.contains("requires") && !text.contains("revision"));
    let mut newer = tx.clone();
    newer.requires = 2;
    newer.revision = 3;
    let newer_bytes = id.encode_record(&newer).unwrap();
    let text = std::str::from_utf8(&newer_bytes).unwrap();
    assert!(text.contains(r#""version":5,"requires":2,"revision":3,"#));
    assert_eq!(id.decode_record(&newer_bytes).unwrap(), newer);
    // Explicit default values are not canonical, so the same record has one byte form.
    let explicit = text.replace(
        r#""requires":2,"revision":3,"#,
        r#""requires":1,"revision":1,"#,
    );
    assert!(id.decode_record(explicit.as_bytes()).is_err());
    for (requires, revision) in [(0, 1), (3, 2), (1, wire::MAX_REVISION + 1)] {
        let mut invalid = tx.clone();
        invalid.requires = requires;
        invalid.revision = revision;
        assert!(id.encode_record(&invalid).is_err());
    }
}

use futures_lite::future::block_on;
fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn edit() -> Change {
    Change {
        id: id(),
        value: Some(BookmarkValue {
            url: "fixture".into(),
            title: "causal fixture".into(),
        }),
    }
}
#[test]
fn invalid_causal_context_chain_and_clock_never_apply() {
    block_on(async {
        let d = tempfile::tempdir().unwrap();
        let owner = Arc::new(Identity::create_space().unwrap());
        let a = Replica::open(d.path().join("a"), owner.clone())
            .await
            .unwrap();
        let member = Arc::new(Identity::join(owner.space.clone()).unwrap());
        let b = Replica::open(d.path().join("b"), member).await.unwrap();
        a.commit(vec![edit()]).await.unwrap();
        let a1 = a.local_objects().await.unwrap().remove(0);
        b.receive(a1.clone()).await.unwrap();
        b.apply_ready().await.unwrap();
        b.commit(vec![edit()]).await.unwrap();
        let b1 = b.local_objects().await.unwrap().remove(0);
        a.receive(b1.clone()).await.unwrap();
        a.apply_ready().await.unwrap();
        a.commit(vec![edit()]).await.unwrap();
        let a2 = a.local_objects().await.unwrap().pop().unwrap();
        for fault in 0..3 {
            let c = Replica::open(
                d.path().join(format!("c{fault}")),
                Arc::new(Identity::join(owner.space.clone()).unwrap()),
            )
            .await
            .unwrap();
            c.receive(a1.clone()).await.unwrap();
            c.receive(b1.clone()).await.unwrap();
            c.apply_ready().await.unwrap();
            let mut tx = owner.decode_record(&a2.bytes).unwrap();
            match fault {
                0 => {
                    tx.context.remove(b.actor());
                    tx.lamport = 2;
                }
                1 => tx.previous = Some("0".repeat(64)),
                _ => tx.lamport += 1,
            }
            // Fault zero needs a dependency that itself mentions B; A3 follows A2's B context.
            if fault == 0 {
                tx.sequence = 3;
                tx.previous = Some(digest(&a2.bytes));
                tx.context.insert(a.actor().into(), 2);
                tx.lamport = 4;
                c.receive(a2.clone()).await.unwrap();
                c.apply_ready().await.unwrap();
            }
            let bytes = owner.encode_record(&tx).unwrap();
            let object = Object {
                filename: tx.filename(&digest(&bytes)),
                bytes,
            };
            c.receive(object).await.unwrap();
            let before = c.snapshot().await.unwrap();
            assert!(c.apply_ready().await.is_err());
            assert_eq!(c.snapshot().await.unwrap(), before);
            c.close().await.unwrap();
        }
        a.close().await.unwrap();
        b.close().await.unwrap();
    });
}
