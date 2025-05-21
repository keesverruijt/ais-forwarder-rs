use directories::ProjectDirs;
use sled::*;
use std::fs;

pub fn get_project_dirs() -> ProjectDirs {
    directories::ProjectDirs::from("net", "verruijt", "ais-forwarder")
        .expect("Cannot find project directories")
}

#[derive(Debug, Clone)]
pub struct Persistence {
    db: Db,
    count: usize,
}

#[allow(dead_code)]
impl Persistence {
    pub fn new() -> Self {
        let project_dirs = get_project_dirs();
        let mut db_path = project_dirs.cache_dir().to_owned();
        fs::create_dir_all(&db_path).expect("Cannot create cache directory");
        db_path.push("location.db");

        let db: Db = sled::Config::default()
            .cache_capacity(500_000)
            .path(&db_path)
            .open()
            .expect(format!("Cannot open database {}", db_path.display()).as_str());
        let count = db.len();

        let this = Persistence { db, count };

        log::debug!("database loaded from {:?}", db_path);

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
