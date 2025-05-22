use std::path::PathBuf;

use sled::*;

#[derive(Debug, Clone)]
pub struct Persistence {
    db: Db,
    count: usize,
}

#[allow(dead_code)]
impl Persistence {
    pub fn new(cache_dir: &str) -> Self {
        let database_path = PathBuf::from(cache_dir);
        if !database_path.exists() {
            std::fs::create_dir_all(&database_path).expect("Cannot create database directory");
        }

        let db: Db = sled::Config::default()
            .cache_capacity(500_000)
            .path(&database_path)
            .open()
            .expect(format!("Cannot open database {}", database_path.display()).as_str());
        let count = db.len();

        let this = Persistence { db, count };

        log::debug!("database loaded from {}", database_path.display());

        this
    }

    pub fn store(&mut self, key: &[u8], value: &[u8]) {
        if self.db.insert(key, value).unwrap().is_none() {
            self.count += 1;
        }
    }

    pub fn iter(&self) -> sled::Iter {
        self.db.iter()
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.db.get(key) {
            Ok(Some(value)) => Some(value.to_vec()),
            Ok(None) => None,
            Err(e) => {
                log::error!("Error getting value from database: {}", e);
                None
            }
        }
    }

    pub fn remove(&mut self, key: &[u8]) {
        if self.db.remove(key).unwrap().is_some() {
            self.count -= 1;
        }
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn clear(&self) {
        self.db.clear().unwrap();
    }

    pub fn count(&self) -> usize {
        self.count
    }
}
