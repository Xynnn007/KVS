use std::path::PathBuf;

use crate::{err::*, KvsEngine};

use sled::Db;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db, 
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let res = self.db.get(key)?
                .map(|iv|iv.to_vec())
                .map(|v|  {
                    String::from_utf8(v)
                })
                .transpose()?;
        Ok(res)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?
                .ok_or(KvsError::NoEntryError)?;
        self.db.flush()?;
        Ok(())
    }
}

impl SledKvsEngine {
    pub fn new(db: Db) -> Result<Self> {
        Ok(Self {
            db
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let db = sled::open(path.into())?;
        Ok(Self {
            db,
        })
    }
}