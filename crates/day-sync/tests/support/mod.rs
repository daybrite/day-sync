// Copyright © The Daybrite Project
// SPDX-License-Identifier: MPL-2.0
#![allow(dead_code)]
use day_sync::*;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
pub struct Fixture {
    pub admin: Replica,
    pub owner: Arc<Identity>,
    identities: Mutex<HashMap<PathBuf, Arc<Identity>>>,
    _dir: tempfile::TempDir,
}
impl Fixture {
    pub async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let owner = Arc::new(Identity::create_space().unwrap());
        let admin = Replica::open(dir.path().join("admin.db"), owner.clone())
            .await
            .unwrap();
        Self {
            admin,
            owner,
            identities: Mutex::new(HashMap::new()),
            _dir: dir,
        }
    }
    pub async fn credentials(&self, path: impl AsRef<Path>) -> Arc<Identity> {
        let existing = self.identities.lock().unwrap().get(path.as_ref()).cloned();
        if let Some(c) = existing {
            return c;
        }
        let credentials = Arc::new(Identity::join(self.owner.space.clone()).unwrap());
        self.identities
            .lock()
            .unwrap()
            .insert(path.as_ref().to_owned(), credentials.clone());
        credentials
    }
    pub fn copy_identity(&self, from: impl AsRef<Path>, to: impl AsRef<Path>) {
        let mut ids = self.identities.lock().unwrap();
        let c = ids[from.as_ref()].clone();
        ids.insert(to.as_ref().to_owned(), c);
    }
}
impl std::fmt::Display for Fixture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.owner.space())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = futures_lite::future::block_on(self.admin.close());
    }
}
pub async fn open(path: impl AsRef<Path>, fixture: &Fixture) -> Result<Replica, Error> {
    Replica::open(path.as_ref(), fixture.credentials(path.as_ref()).await).await
}
pub async fn open_with_probe(
    path: impl AsRef<Path>,
    fixture: &Fixture,
    probe: Probe,
) -> Result<Replica, Error> {
    Replica::open_with_probe(
        path.as_ref(),
        fixture.credentials(path.as_ref()).await,
        probe,
    )
    .await
}
pub async fn deliver_members(from: &Replica, to: &Replica) {
    for control in from.member_objects().await.unwrap() {
        to.receive_member(control).await.unwrap();
    }
}
