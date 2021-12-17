use std::collections::HashMap;
use std::env::current_dir;
use std::io::SeekFrom;
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Write, Seek, BufReader, BufRead, Read, self};

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
    map: HashMap<String, Position>,
    files: HashMap<u64, File>,
    workdir: PathBuf,
    writer: File,
    index: u64,
    compact_size: u64,
}

impl KvsEngine for KvStore {
    fn set(&mut self, k: String, v: String) -> Result<()> {
        self.write_entry(k, v)?;
        
        // if size overflowed
        if self.compact_size > MAX_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&mut self, k: String) -> Result<Option<String>> {
        let escape_k = snailquote::escape(&k).to_string();
        match self.map.get(&escape_k) {
            Some(pos) => {
                self.get_from_file(pos)
            },
            None => {
                Ok(None)
            }
        }
    }

    fn remove(&mut self, k: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        if !self.map.contains_key(&escape_k) {
            Err(ErrorKind::NoEntryError)?
        }

        let e = Entry::Remove(escape_k.clone());
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;
            
        self.writer.write(&bincode).context(ErrorKind::IOError)?;

        self.map.remove(&escape_k);

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
        let mut map = HashMap::new();
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

    fn get_from_file(&self, pos: &Position) -> Result<Option<String>> {
        let mut fd = match self.files.get(&pos.log_no) {
            Some(d) => d,
            None => Err(ErrorKind::LogError)?
        };
        fd.seek(SeekFrom::Start(0))
            .context(ErrorKind::IOError)?;
        let mut reader = BufReader::new(fd);
        reader.seek(SeekFrom::Start(pos.offset))
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

    fn write_entry(&mut self, k: String, v: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        let escape_v = snailquote::escape(&v).to_string();
        let e = Entry::Set(escape_k.clone(), escape_v);
        
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;
        
        let offset = write_entry_data_to_file(&bincode, &mut self.writer)?;

        if let Some(old_pos) = self.map.insert(escape_k, Position{
            log_no: self.index,
            offset,
            size: bincode.len() as u64 + 1,
        }) {
            self.compact_size += old_pos.size;
        }
        
        Ok(())
    }

    fn init_memory_a_file(map: &mut HashMap<String, Position>, log_no: u64, file: &mut File) -> Result<()> {
        file.seek(SeekFrom::Start(0)).context(ErrorKind::IOError)?;

        let mut reader = BufReader::new(file);
        let mut offset  = 0;

        loop {
            let mut line = String::new();
            reader.read_line(&mut line)
                .context(ErrorKind::IOError)?;

            if line.is_empty() || line.ends_with("\r\n") {
                break;
            }

            match bincode::deserialize(line.as_bytes())
                .context(ErrorKind::IOError)? {
                Entry::Set(k1, _) => {
                    map.insert(k1, Position {
                        log_no, 
                        offset,
                        size: line.len() as u64,
                    });
                },
                Entry::Remove(k1) => {
                    map.remove(&k1);
                }
            }
            offset += line.len() as u64;
        }

        reader.seek(SeekFrom::End(0)).context(ErrorKind::IOError)?;
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

fn write_entry_data_to_file(bincode: &Vec<u8>, fd: &mut File) -> Result<u64> {
    let offset = fd.seek(SeekFrom::Current(0))
        .context(ErrorKind::IOError)?;

    fd.write(&bincode).context(ErrorKind::IOError)?;
    fd.write("\n".as_bytes()).context(ErrorKind::IOError)?;
    fd.flush().context(ErrorKind::IOError)?;
    Ok(offset)
}