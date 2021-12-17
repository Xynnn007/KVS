use std::collections::{HashMap, BTreeMap};
use std::env::current_dir;
use std::io::{SeekFrom, Write};
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Seek, BufReader, Read, self};

use failure::ResultExt;
use serde::{Serialize, Deserialize};

use crate::engine::KvsEngine;
use crate::err::*;

#[derive(Serialize, Deserialize)]
enum Entry {
    Set(String, String),
    Remove(String),
}

struct Position {
    log_no: u64, 
    offset: u64,
    size: u64
}

const MAX_SIZE : u64 = 1024 * 1024;
pub struct KvStore {
    map: BTreeMap<String, Position>,
    files: HashMap<u64, File>,
    workdir: PathBuf,
    writer: File,
    index: u64,
    compact_size: u64,
}

impl KvsEngine for KvStore {
    fn set(&mut self, k: String, v: String) -> Result<()> {
        let e = Entry::Set(k.clone(), v);
        let offset = self.writer.seek(SeekFrom::Current(0))
            .context(ErrorKind::IOError)?;
        serde_json::to_writer(&mut self.writer, &e).context(ErrorKind::IOError)?;
        let end = self.writer.seek(SeekFrom::Current(0))
            .context(ErrorKind::IOError)?;
        self.writer.flush()
            .context(ErrorKind::IOError)?;
        if let Some(old_pos) = self.map.insert(k, Position{
            log_no: self.index,
            offset,
            size: end - offset,
        }) {
            self.compact_size += old_pos.size;
        }
        
        // if size overflowed
        if self.compact_size > MAX_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&mut self, k: String) -> Result<Option<String>> {
       if let Some(pos) = self.map.get(&k) {
                let fd = self.files.get_mut(&pos.log_no).ok_or(ErrorKind::IOError)?;
                fd.seek(SeekFrom::Start(pos.offset))
                    .context(ErrorKind::IOError)?;
                let reader = fd.take(pos.size);
                if let Entry::Set(.., value) = serde_json::from_reader(reader).context(ErrorKind::IOError)? { 
                    Ok(Some(value))
                } else {
                    Err(ErrorKind::LogError)?
                }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, k: String) -> Result<()> {
        if !self.map.contains_key(&k) {
            Err(ErrorKind::NoEntryError)?
        }

        let e = Entry::Remove(k.clone());
        serde_json::to_writer(&mut self.writer, &e)
            .context(ErrorKind::IOError)?;

        self.map.remove(&k);

        Ok(())
    }

    fn name(&mut self) -> String {
        "KvStore".to_string()
    }
}

impl KvStore {
    pub fn new() -> Result<Self> {
        KvStore::open(current_dir().context(ErrorKind::IOError)?)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path : PathBuf = path.into();
        let logs = KvStore::get_log_numbers(file_path.clone())?;
        
        let mut files = HashMap::new();
        let mut map = BTreeMap::new();
        let index = logs.last().unwrap_or(&1).clone();
        for it in logs.iter() {
            let mut log_file_path = file_path.clone();
            log_file_path.push(KvStore::get_log_name(*it));
            let mut log_file = OpenOptions::new()
                        .read(true)
                        .open(log_file_path)
                        .context(ErrorKind::IOError)?;
            KvStore::init_memory_a_file(&mut map, *it, &mut log_file)?;
            files.insert(*it, log_file);
        } 

        let mut log_file = file_path.clone();
        log_file.push(KvStore::get_log_name(index));

        let file = OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(current_dir().unwrap().join(log_file.clone()))
                    .context(ErrorKind::IOError)?;
        let reader = OpenOptions::new()
                    .read(true)
                    .open(current_dir().unwrap().join(log_file))
                    .context(ErrorKind::IOError)?;
        files.insert(index, reader);
        
        Ok(Self {
            map,
            writer: file,
            files,
            workdir: file_path,
            index,
            compact_size: 0,
        })
    }

    fn compact(&mut self) -> Result<()> {
        self.index += 1;
        self.open_new_log()?;

        let current_file = &mut self.writer;
        for pos in &mut self.map.values_mut() {
            if let Some(file) = self.files.get_mut(&pos.log_no) {
                
                file.seek(SeekFrom::Start(pos.offset))
                    .context(ErrorKind::IOError)?;
                let mut reader = io::BufReader::new(file.take(pos.size));
                let offset = current_file.seek(SeekFrom::Current(0))
                    .context(ErrorKind::IOError)?;
                io::copy(&mut reader, current_file)
                    .context(ErrorKind::IOError)?;
                
                *pos = Position {
                    log_no: self.index,
                    offset,
                    size: pos.size
                };

                file.seek(SeekFrom::End(0))
                    .context(ErrorKind::IOError)?;
            }
        }

        self.delete_old_logs()?;
        self.index += 1;
        self.open_new_log()?;
        self.compact_size = 0;
        Ok(())
    }

    fn delete_old_logs(&mut self) -> Result<()> {
        let logs = KvStore::get_log_numbers(self.workdir.clone())?;
        for log in logs.iter() {
            if *log < self.index {
                let log_name = KvStore::get_log_name(*log);
                self.files.remove(log);
                let mut log_path= self.workdir.clone();
                log_path.push(log_name);
                fs::remove_file(log_path)
                    .context(ErrorKind::IOError)?;
            }
        }
        Ok(())
    }

    fn open_new_log(&mut self) -> Result<()> {
        let mut file_path = self.workdir.clone();
        file_path.push(KvStore::get_log_name(self.index));
        let file = OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(file_path.clone())
                    .context(ErrorKind::IOError)?;
        self.writer = file;
        let reader = OpenOptions::new()
            .read(true)
            .open(file_path)
            .context(ErrorKind::IOError)?;
        self.files.insert(self.index, reader);
            
        Ok(())
    }

    fn get_log_name(id: u64) -> String {
        format!("{}.log", id)
    }

    fn init_memory_a_file(map: &mut BTreeMap<String, Position>, log_no: u64, file: &mut File) -> Result<()> {
        file.seek(SeekFrom::Start(0)).context(ErrorKind::IOError)?;

        let reader = BufReader::new(file);
        let mut offset  = 0;

        let mut stream = serde_json::Deserializer::from_reader(reader)
            .into_iter::<Entry>();

        while let Some(e) = stream.next() {
            let new_pow = stream.byte_offset() as u64;
            match e.context(ErrorKind::SerializeError)? {
                Entry::Set(k1, _) => {
                    map.insert(k1, Position {
                        log_no, 
                        offset,
                        size: new_pow  - offset,
                    });
                },
                Entry::Remove(k1) => {
                    map.remove(&k1);
                }
            }
            offset = new_pow;
        }
        Ok(())
    }

    fn get_log_numbers(file_path: PathBuf) -> Result<Vec<u64>> {     
        let mut logs: Vec<u64> = fs::read_dir(file_path).context(ErrorKind::IOError)?
            .flat_map(|f| -> Result<_> {Ok(f.context(ErrorKind::IOError)?.path())})
            .filter(|f| {f.is_file()})
            .filter(|f|f.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|s|s.to_str())
                    .map(|s| s.trim_end_matches(".log"))
                    .map(|i| i.parse::<u64>())
            })
            .flatten()
            .collect();

        logs.sort_unstable();
        Ok(logs)
    }
}