use std::collections::{HashMap, BTreeMap};
use std::env::current_dir;
use std::io::{SeekFrom, Write, BufWriter};
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Seek, BufReader, Read, self};
use std::sync::{Arc, Mutex};

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
    map: Arc<Mutex<BTreeMap<String, Position>>>,
    readers: Arc<Mutex<HashMap<u64, ReadSeeker<File>>>>,
    workdir: Arc<Mutex<PathBuf>>,
    writer: Arc<Mutex<WriteSeeker<File>>>,
    index: Arc<Mutex<u64>>,
    compact_size: Arc<Mutex<u64>>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { 
            map: self.map.clone(), 
            readers: self.readers.clone(), 
            workdir: self.workdir.clone(), 
            writer: self.writer.clone(), 
            index: self.index.clone(), 
            compact_size: self.compact_size.clone() 
        }
    }
}

macro_rules! atomic {
    ($expr: expr) => {
        $expr.clone().lock().unwrap()
    };
}

macro_rules! atomic_ref {
    ($expr: expr) => {
        *($expr.clone().lock().unwrap())
    };
}

macro_rules! atomic_new {
    ($expr: expr) => {
        Arc::new(Mutex::new($expr))
    };
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, v: String) -> Result<()> {
        let e = Entry::Set(k.clone(), v);
        let offset: u64;
        let end: u64;
        let writer = self.writer.clone();
        {
            let mut writer = writer.lock().unwrap();
            offset = writer.pos as u64;
            serde_json::to_writer(&mut *writer, &e).context(ErrorKind::IOError)?;
            writer.flush()
                .context(ErrorKind::IOError)?;
            end = writer.pos as u64;
        }
        if let Some(old_pos) = atomic!(self.map).insert(k, Position{
            log_no: atomic_ref!(self.index),
            offset,
            size: end - offset,
        }) {
            atomic_ref!(self.compact_size) += old_pos.size;
        }
        
        // if size overflowed
        if atomic_ref!(self.compact_size) > MAX_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&self, k: String) -> Result<Option<String>> {
       if let Some(pos) = atomic!(self.map).get(&k) {
            let readers_mutex = self.readers.clone();
            let mut readers = readers_mutex.lock().unwrap();
            let reader = readers.get_mut(&pos.log_no).ok_or(ErrorKind::IOError)?;
            reader.seek(SeekFrom::Start(pos.offset))
                .context(ErrorKind::IOError)?;
            let reader = reader.take(pos.size);

            if let Entry::Set(.., value) = serde_json::from_reader(reader)
                .context(ErrorKind::IOError)? { 
                Ok(Some(value))
            } else {
                Err(ErrorKind::LogError)?
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, k: String) -> Result<()> {
        if !atomic!(self.map).contains_key(&k) {
            Err(ErrorKind::NoEntryError)?
        }

        let e = Entry::Remove(k.clone());
        serde_json::to_writer(&mut atomic_ref!(self.writer), &e)
            .context(ErrorKind::IOError)?;
        atomic!(self.writer).flush()
            .context(ErrorKind::IOError)?;
            
        atomic!(self.map).remove(&k);

        Ok(())
    }
}

impl KvStore {
    pub fn new() -> Result<Self> {
        KvStore::open(current_dir().context(ErrorKind::IOError)?)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path : PathBuf = path.into();
        let logs = KvStore::get_log_numbers(file_path.clone())?;
        
        let mut readers = HashMap::new();
        let mut map = BTreeMap::new();
        let index = *logs.last().unwrap_or(&1);
        for it in logs.iter() {
            let mut log_file_path = file_path.clone();
            log_file_path.push(KvStore::get_log_name(*it));
            let mut reader = ReadSeeker::new(OpenOptions::new()
                        .read(true)
                        .open(log_file_path)
                        .context(ErrorKind::IOError)?);
            
            KvStore::init_memory_a_file(&mut map, *it, &mut reader)?;
            readers.insert(*it, reader);
        } 

        let mut log_file = file_path.clone();
        log_file.push(KvStore::get_log_name(index));

        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(current_dir().unwrap().join(log_file.clone()))
                    .context(ErrorKind::IOError)?);
        let reader = ReadSeeker::new(OpenOptions::new()
                    .read(true)
                    .open(current_dir().unwrap().join(log_file))
                    .context(ErrorKind::IOError)?);
        readers.insert(index, reader);
        
        Ok(Self {
            map: atomic_new!(map),
            writer: atomic_new!(writer),
            readers: atomic_new!(readers),
            workdir: atomic_new!(file_path),
            index: atomic_new!(index),
            compact_size: atomic_new!(0),
        })
    }

    fn compact(&self) -> Result<()> {
        atomic_ref!(self.index) += 1;
        self.open_new_log()?;

        for pos in &mut atomic!(self.map).values_mut() {
            if let Some(file) = atomic!(self.readers).get_mut(&pos.log_no) {
                file.seek(SeekFrom::Start(pos.offset))
                    .context(ErrorKind::IOError)?;
                let mut reader = io::BufReader::new(file.take(pos.size));
                let offset = atomic_ref!(self.writer).seek(SeekFrom::Current(0))
                    .context(ErrorKind::IOError)?;
                io::copy(&mut reader, &mut atomic_ref!(self.writer))
                    .context(ErrorKind::IOError)?;
                
                *pos = Position {
                    log_no: atomic_ref!(self.index),
                    offset,
                    size: pos.size
                };

                file.seek(SeekFrom::End(0))
                    .context(ErrorKind::IOError)?;
            }
        }

        self.delete_old_logs()?;
        atomic_ref!(self.index) += 1;
        self.open_new_log()?;
        atomic_ref!(self.compact_size) = 0;
        Ok(())
    }

    fn delete_old_logs(&self) -> Result<()> {
        let logs = KvStore::get_log_numbers(atomic!(self.workdir).clone())?;
        for log in logs.iter() {
            if *log < atomic_ref!(self.index) {
                let log_name = KvStore::get_log_name(*log);
                atomic!(self.readers).remove(log);
                let mut log_path= atomic!(self.workdir).clone();
                log_path.push(log_name);
                fs::remove_file(log_path)
                    .context(ErrorKind::IOError)?;
            }
        }
        Ok(())
    }

    fn open_new_log(&self) -> Result<()> {
        let mut file_path = atomic!(self.workdir).clone();
        file_path.push(KvStore::get_log_name(atomic_ref!(self.index)));
        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(file_path.clone())
                    .context(ErrorKind::IOError)?);
        atomic_ref!(self.writer) = writer;
        let reader = ReadSeeker::new(OpenOptions::new()
            .read(true)
            .open(file_path)
            .context(ErrorKind::IOError)?);
        atomic!(self.readers).insert(atomic_ref!(self.index), reader);
            
        Ok(())
    }

    fn get_log_name(id: u64) -> String {
        format!("{}.log", id)
    }

    fn init_memory_a_file<R: Read + Seek + Sync>(map: &mut BTreeMap<String, Position>, log_no: u64, reader: &mut ReadSeeker<R>) -> Result<()> {
        reader.seek(SeekFrom::Start(0)).context(ErrorKind::IOError)?;

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

struct ReadSeeker <R: Read + Seek> {
    reader: BufReader<R>,
    pos: usize,
}

impl<R: Read + Seek + Sync> ReadSeeker<R> {
    fn new(reader: R) -> Self {
        Self {
            pos: 0,
            reader: BufReader::new(reader),
        }
    }
}

impl<R: Read + Seek + Sync> Read for ReadSeeker<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::result::Result<usize, io::Error> {
        let len = self.reader.read(buf)?;
        self.pos += len;
        Ok(len)
    }
}

impl<R: Read + Seek + Sync> Seek for ReadSeeker<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.pos = self.reader.seek(pos)? as usize;
        Ok(self.pos as u64)
    }
}

struct WriteSeeker <W: Write + Seek + Sync> {
    writer: BufWriter<W>,
    pos: usize,
}

impl<W: Write + Seek + Sync> WriteSeeker<W> {
    fn new(writer: W) -> Self {
        Self {
            pos: 0,
            writer: BufWriter::new(writer),
        }
    }
}

impl<R: Write + Seek + Sync> Write for WriteSeeker<R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek + Sync> Seek for WriteSeeker<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::result::Result<u64, std::io::Error> {
        self.pos = self.writer.seek(pos)? as usize;
        Ok(self.pos as u64)
    }
}