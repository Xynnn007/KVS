use std::{env::current_dir, path::PathBuf};

use crate::{err::*, KvsEngine};

use failure::ResultExt;
use sled::Db;

pub struct SledKvsEngine {
    db: Db, 
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let _ = self.db.insert(key, value.as_bytes())
            .context(ErrorKind::SledError)?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
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

    fn remove(&mut self, key: String) -> Result<()> {
        let _ = self.db.remove(key)
            .context(ErrorKind::SledError)?;
        Ok(())
    }

    fn name(&mut self) -> String {
        "SledKvsEngine".to_string()
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
            db
        })
    }
}