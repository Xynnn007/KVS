use std::path::PathBuf;

use crate::{err::*, KvsEngine};

use failure::ResultExt;
use sled::Db;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db, 
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())
            .context(ErrorKind::SledError)?;
        self.db.flush().context(ErrorKind::SledError)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let res = self.db.get(key).context(ErrorKind::SledError)?
                .map(|iv|iv.to_vec())
                .map(|v|  {
                    String::from_utf8(v)
                })
                .transpose()
                .context(ErrorKind::Utf8Error)?;
        Ok(res)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)
                .context(ErrorKind::SledError)?
                .ok_or(ErrorKind::NoEntryError)?;
        self.db.flush()
                .context(ErrorKind::SledError)?;
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
        let db = sled::open(path.into())
            .context(ErrorKind::IOError)?;
        Ok(Self {
            db,
        })
    }
}