use std::env::current_dir;
use std::{collections::HashMap, io::SeekFrom};
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::io::{Write, Seek, BufReader, BufRead};

use failure::ResultExt;
use serde::{Serialize, Deserialize};
use crate::err::*;
pub struct KvStore {
    map: HashMap<String, u64>,
    file: File,
}

#[derive(Serialize, Deserialize)]
enum Entry {
    Set(String, String),
    Remove(String),
}

const STORAGE : &str = "storage.bin";

impl KvStore {
    pub fn new() -> Result<Self> {
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .read(true)
            .create(true)
            .open(current_dir().unwrap().join(STORAGE))
            .context(ErrorKind::IOError)?;

        file.seek(SeekFrom::End(0)).context(ErrorKind::IOError)?;

        let mut r = Self {
            map : HashMap::new(),
            file,
        };

        r.init()?;
        Ok(r)
    }

    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        let escape_v = snailquote::escape(&v).to_string();
        let e = Entry::Set(escape_k.clone(), escape_v);
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;

        let offset = self.file.seek(SeekFrom::Current(0))
            .context(ErrorKind::IOError)?;

        self.file.write(&bincode).context(ErrorKind::IOError)?;
        self.file.write("\n".as_bytes()).context(ErrorKind::IOError)?;

        self.map.insert(escape_k, offset);

        Ok(())
    }

    pub fn get(&mut self, k: String) -> Result<Option<String>> {
        let escape_k = snailquote::escape(&k).to_string();
        match self.map.get(&escape_k) {
            Some(t) => {
                let index = t.clone();
                self.get_from_file(index)
            },
            None => {
                Ok(None)
            }
        }
    }

    pub fn remove(&mut self, k: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        if !self.map.contains_key(&escape_k) {
            Err(ErrorKind::NoEntryError)?
        }

        let e = Entry::Remove(escape_k.clone());
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;
            
        self.file.write(&bincode).context(ErrorKind::IOError)?;

        self.map.remove(&escape_k);

        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let mut file_path : PathBuf = path.into();
        file_path.push(STORAGE);
        let mut file = OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .read(true)
                    .create(true)
                    .open(current_dir().unwrap().join(file_path))
                    .context(ErrorKind::IOError)?;

        file.seek(SeekFrom::End(0)).context(ErrorKind::IOError)?;
        let mut r = Self {
            file,
            map : HashMap::new(),
        };
        r.init()?;
        Ok(r)
    }

    fn init(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0)).context(ErrorKind::IOError)?;

        let mut reader = BufReader::new(&mut self.file);
        let mut offset  = 0;

        loop {
            let mut line = String::new();
            reader.read_line(&mut line)
                .context(ErrorKind::IOError)?;

            if line.len() == 0 {
                break;
            }
            match bincode::deserialize(line.as_bytes())
                .context(ErrorKind::IOError)? {
                Entry::Set(k1, _) => {
                    self.map.insert(k1, offset);
                },
                Entry::Remove(k1) => {
                    self.map.remove(&k1);
                }
            }
            offset += line.len() as u64;
        }

        self.file.seek(SeekFrom::End(0)).context(ErrorKind::IOError)?;
        Ok(())
    }

    fn get_from_file(&mut self, offset: u64) -> Result<Option<String>> {
        let mut reader = BufReader::new(&self.file);
        reader.seek(SeekFrom::Start(offset))
            .context(ErrorKind::IOError)?;
        match bincode::deserialize_from(reader)
            .context(ErrorKind::IOError)? {
                Entry::Set(_, v) => {
                    let r = snailquote::unescape(&v)
                        .context(ErrorKind::IOError)?;
                    Ok(Some(r))
                },
                Entry::Remove(_) => {
                    Err(ErrorKind::LogError)?
                }
        }
    }
}
