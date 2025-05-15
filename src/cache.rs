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
}

#[allow(dead_code)]
impl Persistence {
    pub fn new() -> Self {
        let project_dirs = get_project_dirs();
        let mut db_path = project_dirs.config_dir().to_owned();
        fs::create_dir_all(&db_path).expect("Cannot create settings directory");
        db_path.push("location.db");

        let db: Db = sled::open(&db_path).unwrap();

        let this = Persistence { db };

        log::debug!("database loaded from {:?}", db_path);

        this
    }

    pub fn store(&self, key: &[u8], value: &[u8]) {
        self.db.insert(key, value).unwrap();
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

    pub fn remove(&self, key: &[u8]) {
        self.db.remove(key).unwrap();
    }

    pub fn flush(&self) {
        self.db.flush().unwrap();
    }

    pub fn clear(&self) {
        self.db.clear().unwrap();
    }
}
