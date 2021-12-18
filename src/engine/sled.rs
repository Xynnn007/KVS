use std::{env::current_dir, path::PathBuf, sync::{Arc, Mutex}};

use crate::{err::*, KvsEngine};

use failure::ResultExt;
use sled::Db;

pub struct SledKvsEngine {
    db: Arc<Mutex<Db>>, 
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
        let db = self.db.clone();
        {
            let mu_db = db.lock().unwrap();
            mu_db.insert(key, value.as_bytes())
            .context(ErrorKind::SledError)?;
        }

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let db = self.db.clone();
        {
            let mu_db = db.lock().unwrap();
            match mu_db.get(key)
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
    }

    fn remove(&self, key: String) -> Result<()> {
        let db = self.db.clone();
        {
            let mu_db = db.lock().unwrap();
            mu_db.remove(key)
                .context(ErrorKind::SledError)?;
        }

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
            db: Arc::new(Mutex::new(db)),
        })
    }
}