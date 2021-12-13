use std::collections::HashMap;
use std::env::current_dir;
use std::io::SeekFrom;
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Write, Seek, BufReader, BufRead, Read, self};

use failure::ResultExt;
use serde::{Serialize, Deserialize};

use crate::err::*;

#[derive(Serialize, Deserialize)]
enum Entry {
    Set(String, String),
    Remove(String),
}

pub struct Position {
    key: String, 
    offset: u64,
    size: u64
}

const MAX_SIZE : u64 = 1024;
pub struct KvStore {
    map: HashMap<String, Position>,
    files: HashMap<String, File>,
    workdir: PathBuf,
    file:String,
    index: u64,
    compact_size: u64,
}


impl KvStore {
    pub fn new() -> Result<Self> {
        KvStore::open(current_dir().context(ErrorKind::IOError)?)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path : PathBuf = path.into();
        let (mut logs, mut clogs) = KvStore::get_log_numbers(file_path.clone())?;
        clogs.reverse();
        let mut compacted_log = None;
        let mut compacted_log_index = None;
        for clog in clogs.iter() {
            let mut clog_file_path = file_path.clone();
            clog_file_path.push(KvStore::get_compacted_log_name(*clog));
            let mut file = OpenOptions::new()
                    .truncate(false)
                    .read(true)
                    .open(clog_file_path)
                    .context(ErrorKind::IOError)?;
            if legel_compacted_log(&mut file)? {
                compacted_log = Some(file);
                compacted_log_index = Some(*clog);
                break;
            }
        }

        logs.sort_unstable();

        let last_log_index = logs.last();
        let last_clog_index = clogs.last();
        let index : u64 = last_log_index.unwrap_or(&0)
            + last_clog_index.unwrap_or(&0) + 1;
        
        let mut files = HashMap::new();
        let mut map = HashMap::new();

        match compacted_log_index {
            None => {
                for it in logs.iter() {
                    let mut log_file = file_path.clone();
                    log_file.push(KvStore::get_log_name(*it));
                    let mut file = OpenOptions::new()
                                .truncate(false)
                                .read(true)
                                .open(log_file)
                                .context(ErrorKind::IOError)?;
                    KvStore::init_memory_a_file(&mut map, &KvStore::get_log_name(*it)[..], &mut file)?;
                    files.insert(KvStore::get_log_name(*it), file);
                } 
            },
            Some(compacted_log_index) => {
                let compacted_log_name = KvStore::get_compacted_log_name(compacted_log_index);
                let mut compacted_log_file = compacted_log.unwrap();
                KvStore::init_memory_a_file(&mut map,&compacted_log_name, &mut compacted_log_file)?;

                files.insert(compacted_log_name, compacted_log_file);
                for it in logs.iter() {
                    if *it < compacted_log_index {
                        continue
                    }

                    let mut log_file_path = file_path.clone();
                    log_file_path.push(KvStore::get_log_name(*it));
                    let mut log_file = OpenOptions::new()
                                .write(true)
                                .truncate(false)
                                .read(true)
                                .create(true)
                                .open(log_file_path)
                                .context(ErrorKind::IOError)?;
                    KvStore::init_memory_a_file(&mut map, &KvStore::get_log_name(*it), &mut log_file)?;
                    files.insert(KvStore::get_log_name(*it), log_file);
                } 
            }
        } 
        
        let mut log_file = file_path.clone();
        log_file.push(KvStore::get_log_name(index));
        let file = OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .read(true)
                    .create(true)
                    .open(current_dir().unwrap().join(log_file))
                    .context(ErrorKind::IOError)?;
        files.insert(KvStore::get_log_name(index), file);
        Ok(Self {
            map,
            file: KvStore::get_log_name(index),
            files,
            workdir: file_path,
            index,
            compact_size: 0,
        })
    }

    pub fn set(&mut self, k: String, v: String) -> Result<()> {
        self.write_entry(k, v)?;
        
        // if size overflowed
        if self.compact_size > MAX_SIZE {
            self.compaction()?;
        }

        Ok(())
    }

    pub fn get(&mut self, k: String) -> Result<Option<String>> {
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

    pub fn remove(&mut self, k: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        if !self.map.contains_key(&escape_k) {
            Err(ErrorKind::NoEntryError)?
        }

        let e = Entry::Remove(escape_k.clone());
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;
            
        self.file()?.write(&bincode).context(ErrorKind::IOError)?;

        self.map.remove(&escape_k);

        Ok(())
    }

    fn get_from_file(&self, pos: &Position) -> Result<Option<String>> {
        let mut fd = match self.files.get(&pos.key) {
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

    fn file(&mut self) -> Result<&mut File> {
        match self.files.get_mut(&self.file) {
            Some(f) => Ok(f),
            None => Err(ErrorKind::IOError)?
        }
    }

    // fn get_file_by_name(&mut self, name: &str) -> Result<&mut File> {
    //     match 
    // }

    fn compaction(&mut self) -> Result<()> {
        self.index += 1;
        let mut compacted_log = self.open_new_compacted_log()?;
        
        let mut new_map = HashMap::new();
        for (k,pos) in &self.map {
            if let Some(file) = self.files.get_mut(&pos.key) {
                file.seek(SeekFrom::Start(pos.offset))
                    .context(ErrorKind::IOError)?;
                let mut reader = io::BufReader::new(file.take(pos.size));
                let offset = compacted_log.seek(SeekFrom::Current(0))
                    .context(ErrorKind::IOError)?;
                io::copy(&mut reader, &mut compacted_log)
                    .context(ErrorKind::IOError)?;

                let new_pos = Position {
                    key: KvStore::get_compacted_log_name(self.index),
                    offset,
                    size: pos.size
                };
            
                new_map.insert(k.clone(), new_pos);
                file.seek(SeekFrom::End(0))
                    .context(ErrorKind::IOError)?;
            }
        }
        compacted_log.write("\r\n".as_bytes()).context(ErrorKind::IOError)?;

        self.map = new_map;
        self.files.insert(KvStore::get_compacted_log_name(self.index), compacted_log);

        self.delete_old_logs()?;
        self.index += 1;
        self.open_new_log()?;
        self.compact_size = 0;
        Ok(())
    }

    fn delete_old_logs(&mut self) -> Result<()> {
        let (logs, clogs) = KvStore::get_log_numbers(self.workdir.clone())?;
        for log in logs.iter() {
            if *log < self.index {
                let log_name = KvStore::get_log_name(*log);
                self.files.remove(&log_name);
                let mut log_path= self.workdir.clone();
                log_path.push(log_name);
                fs::remove_file(log_path)
                    .context(ErrorKind::IOError)?;
            }
        }
        for clog in clogs.iter() {
            if *clog < self.index {
                let clog_name = KvStore::get_compacted_log_name(*clog);
                self.files.remove(&clog_name);
                let mut clog_path= self.workdir.clone();
                clog_path.push(clog_name);
                fs::remove_file(clog_path)
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
                    .read(true)
                    .create(true)
                    .open(file_path)
                    .context(ErrorKind::IOError)?;
        self.files.insert(KvStore::get_log_name(self.index), file);
        self.file = KvStore::get_log_name(self.index);
        Ok(())
    }

    fn open_new_compacted_log(&mut self) -> Result<File> {
        let mut file_path = self.workdir.clone();
        let log_name = KvStore::get_compacted_log_name(self.index);
        file_path.push(log_name);
        Ok(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .read(true)
                    .create(true)
                    .open(file_path)
                    .context(ErrorKind::IOError)?)
    }

    fn get_log_name(id: u64) -> String {
        format!("{}.log", id)
    }

    fn get_compacted_log_name(id: u64) -> String {
        format!("{}.compacted", id)
    }

    fn write_entry(&mut self, k: String, v: String) -> Result<()> {
        let escape_k = snailquote::escape(&k).to_string();
        let escape_v = snailquote::escape(&v).to_string();
        let e = Entry::Set(escape_k.clone(), escape_v);
        
        let bincode = bincode::serialize(&e).context(ErrorKind::IOError)?;
        
        let offset = write_entry_data_to_file(&bincode, self.file()?)?;

        if let Some(old_pos) = self.map.insert(escape_k, Position{
            key: KvStore::get_log_name(self.index),
            offset,
            size: bincode.len() as u64 + 1,
        }) {
            self.compact_size += old_pos.size;
        }
        
        Ok(())
    }

    fn init_memory_a_file(map: &mut HashMap<String, Position>, filename: &str, file: &mut File) -> Result<()> {
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
                        key: filename[..].to_string(), 
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

    fn get_log_numbers(file_path: PathBuf) -> Result<(Vec<u64>,Vec<u64>)> {
        let mut clogs: Vec<u64> = fs::read_dir(file_path.clone()).context(ErrorKind::IOError)?
            .flat_map(|f| -> Result<_> {Ok(f.context(ErrorKind::IOError)?.path())})
            .filter(|f| {f.is_file()})
            .filter(|f|f.extension() == Some("compacted".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|s|s.to_str())
                    .map(|s| s.trim_end_matches(".compacted"))
                    .map(|i| i.parse::<u64>())
            })
            .flatten()
            .collect();
        
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

        clogs.sort_unstable();
        logs.sort_unstable();
        Ok((logs, clogs))
    }
}

fn legel_compacted_log(fd: &mut File) -> Result<bool> {
    fd.seek(SeekFrom::End(-2)).context(ErrorKind::IOError)?;
    let mut buf = [0, 0];
    fd.read(&mut buf).context(ErrorKind::IOError)?;
    Ok(buf == [0xd, 0xa])
    // \r\n as end of compacted file
}

fn write_entry_data_to_file(bincode: &Vec<u8>, fd: &mut File) -> Result<u64> {
    let offset = fd.seek(SeekFrom::Current(0))
        .context(ErrorKind::IOError)?;

    fd.write(&bincode).context(ErrorKind::IOError)?;
    fd.write("\n".as_bytes()).context(ErrorKind::IOError)?;
    fd.flush().context(ErrorKind::IOError)?;
    Ok(offset)
}