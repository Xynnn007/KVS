use std::{env::current_dir, path::PathBuf, sync::{Arc, Mutex}};

use crate::{err::*, KvsEngine};

use failure::ResultExt;
use sled::Db;

pub struct SledKvsEngine {
    db: Db, 
}

impl Clone for SledKvsEngine {
    fn clone(&self) -> Self {
        Self { 
            db: self.db.clone(), 
        }
    }
}

impl KvsEngine for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.as_bytes())
            .context(ErrorKind::SledError)?;

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.db.get(key)
            .context(ErrorKind::SledError)? {
            Some(iv) => {
                let str = String::from_utf8(iv.to_vec())
                    .context(ErrorKind::Utf8Error)?;
                Ok(Some(str))
            },
            None => {
                Ok(None)
            }
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        if !self.db.contains_key(&key).context(ErrorKind::SledError)? {
            Err(ErrorKind::NoEntryError)?
        }

        self.db.remove(key)
                .context(ErrorKind::SledError)?;

        Ok(())
    }
}

impl SledKvsEngine {
    pub fn new() -> Result<Self> {
        SledKvsEngine::open(current_dir().context(ErrorKind::IOError)?)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let db = sled::open(path.into())
            .context(ErrorKind::IOError)?;
        Ok(Self {
            db,
        })
    }
}