use std::cell::RefCell;
use std::collections::BTreeMap;
use std::env::current_dir;
use std::io::{SeekFrom, Write, BufWriter, Take};
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Seek, BufReader, Read, self};
use std::sync::atomic::{AtomicU64,Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use serde::{Serialize, Deserialize};
use crossbeam_skiplist::SkipMap;

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
    workdir: Arc<PathBuf>,
    reader: ReadModule,
    writer: Arc<Mutex<WriteModule>>,
    map: Arc<SkipMap<String, Position>>,
}

struct ReadModule {
    readers: RefCell<BTreeMap<u64, ReadSeeker<File>>>,
    newest_index: Arc<AtomicU64>, 
    workdir: Arc<PathBuf>
}

impl ReadModule {
    fn new(newest_index : Arc<AtomicU64>, workdir: Arc<PathBuf>) -> Self {
        Self {
            readers: RefCell::new(BTreeMap::new()),
            newest_index,
            workdir,
        }
    }
}

struct WriteModule {
    reader: ReadModule,
    map: Arc<SkipMap<String, Position>>,
    writer: WriteSeeker<File>,
    index: u64,
    workdir: Arc<PathBuf>,
    compact_size: u64,
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, v: String) -> Result<()> {
        self.writer.lock().unwrap().set(k, v)
    }

    fn get(&self, k: String) -> Result<Option<String>> {
        if let Some(pos) = self.map.get(&k) {
            Ok (self.reader.read(pos.value())?)
        } else {
            Ok(None)
        }
    }

    fn remove(&self, k: String) -> Result<()> {
        self.writer.lock().unwrap().remove(k)
    }
}

impl WriteModule {
    fn set(&mut self, k: String, v: String) -> Result<()> {
        let e = Entry::Set(k.clone(), v);

        let offset = self.writer.pos as u64;
        serde_json::to_writer(&mut self.writer, &e)?;
        self.writer.flush()?;
        let end = self.writer.pos as u64;
        if let Some(old_pos) = self.map.get(&k) {
            self.compact_size += old_pos.value().size;
        }

        self.map.insert(k, Position{
            log_no: self.index,
            offset,
            size: end - offset,
        });

        // if size overflowed
        if self.compact_size > MAX_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn remove(&mut self, k: String) -> Result<()> {
        let e = Entry::Remove(k.clone());
        
        serde_json::to_writer(&mut self.writer, &e)?;
        self.writer.flush()?;
        
        if let None = self.map.remove(&k) {
            Err(KvsError::NoEntryError)?
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        self.index += 1;
        self.open_new_log()?;
        let mut offset = 0;
        for e in self.map.iter() {
            let len = self.reader.read_and(&e.value(), |mut f| {
                Ok(io::copy(&mut f, &mut self.writer)?)
            })?;
            self.map.insert(e.key().clone(), Position {
                log_no: self.index,
                offset,
                size: len
            });
            offset += len;
        }

        self.writer.flush()?;
        self.delete_old_logs()?;

        self.index += 1;
        self.open_new_log()?;
        
        self.compact_size = 0;
        Ok(())
    }

    fn open_new_log(&mut self) -> Result<()> {
        let mut file_path = (*self.workdir).clone();
        file_path.push(get_log_name(self.index));

        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(file_path.as_path())?);
        let reader = ReadSeeker::new(OpenOptions::new()
                    .read(true)
                    .open(file_path.as_path())?);

        self.writer = writer;
        self.reader.readers.borrow_mut().insert(self.index, reader);

        Ok(())
    }

    fn delete_old_logs(&self) -> Result<()> {
        let logs = get_log_numbers(self.workdir.to_path_buf())?;
        let mut delete_files = vec![];

        for log in logs.iter() {
            if *log < self.index {
                let log_name = get_log_name(*log);
                self.reader.readers.borrow_mut().remove(log);
                let mut log_path= (*self.workdir).clone();
                log_path.push(log_name);
                delete_files.push(log_path);
            }
        }

        for f in delete_files {
            fs::remove_file(f.as_path())?;
        }

        (*self.reader.newest_index).store(self.index, SeqCst);

        Ok(())
    }
}

impl ReadModule {
    fn read(&self, pos: &Position) -> Result<Option<String>> {
        self.read_and(pos, |f| {
            if let Entry::Set(..,value) = serde_json::from_reader(f)? {
                Ok(Some(value))
            } else {
                Ok(None)
            }
        })
    }

    fn read_and<F, R>(&self, pos: &Position, f: F) -> Result<R>
    where F: FnOnce(Take<&mut ReadSeeker<File>>) -> Result<R>
    {
        self.update_readers()?;
        let mut readers = self.readers.borrow_mut();
        
        if !readers.contains_key(&pos.log_no) {
            let mut file_path = (*self.workdir).clone();
            file_path.push(get_log_name(pos.log_no));
            let reader = ReadSeeker::new(OpenOptions::new()
                .read(true)
                .open(file_path.as_path())?);
            readers.insert(pos.log_no, reader);
        }

        if let Some(reader) = readers.get_mut(&pos.log_no) {
            reader.seek(SeekFrom::Start(pos.offset))?;
            let r = reader.take(pos.size);
            f(r)
        } else {
            Err(KvsError::IOError(io::Error::new(
                io::ErrorKind::Other, 
                format!("No reader opened for {}", pos.log_no)
            )))?
        }
    }

    fn update_readers(&self) -> Result<()> {
        let mut readers = self.readers.borrow_mut();
        while !readers.is_empty() {
            let key = *readers.keys().next().unwrap();
            if key < self.newest_index.load(SeqCst) {
                readers.remove(&key);
            } else {
                break
            }
        }
        Ok(())
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { 
            workdir: Arc::clone(&self.workdir), 
            reader:  ReadModule {
                readers: RefCell::new(BTreeMap::new()),
                newest_index: Arc::clone(&self.reader.newest_index),
                workdir: Arc::clone(&self.reader.workdir),
            }, 
            writer: self.writer.clone(),
            map: Arc::clone(&self.map), 
        }
    }
}

impl KvStore {
    pub fn new() -> Result<Self> {
        KvStore::open(current_dir()?)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path : PathBuf = path.into();
        let logs = get_log_numbers(file_path.clone())?;
        
        let mut map = SkipMap::new();
        let index = *logs.last().unwrap_or(&1);
        for it in logs.iter() {
            let mut log_file_path = file_path.clone();
            log_file_path.push(get_log_name(*it));
            let mut reader = ReadSeeker::new(OpenOptions::new()
                        .read(true)
                        .open(log_file_path)?);
            
            init_memory_a_file(&mut map, *it, &mut reader)?;
        } 

        let mut log_file = file_path.clone();
        log_file.push(get_log_name(index));

        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(current_dir().unwrap().join(log_file.clone()))?);
        
        let ato_index = Arc::new(AtomicU64::new(index));
        let workdir = Arc::new(file_path);
        let map = Arc::new(map);
        Ok(Self {
            map: Arc::clone(&map),
            writer: Arc::new(Mutex::new(WriteModule {
                writer,
                index,
                compact_size: 0,
                reader: ReadModule::new(Arc::clone(&ato_index), Arc::clone(&workdir)),
                map: Arc::clone(&map),
                workdir: Arc::clone(&workdir),
            })),
            reader: ReadModule::new(Arc::clone(&ato_index), Arc::clone(&workdir)),
            workdir: Arc::clone(&workdir),
        })
    }
}

fn init_memory_a_file<R: Read + Seek + Sync>(map: &mut SkipMap<String, Position>, log_no: u64, reader: &mut ReadSeeker<R>) -> Result<()> {
    reader.seek(SeekFrom::Start(0))?;

    let mut offset  = 0;
    let mut stream = serde_json::Deserializer::from_reader(reader)
        .into_iter::<Entry>();

    while let Some(e) = stream.next() {
        let new_pow = stream.byte_offset() as u64;
        match e? {
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
    let mut logs: Vec<u64> = fs::read_dir(file_path)?
        .flat_map(|f| -> Result<_> {Ok(f?.path())})
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

fn get_log_name(id: u64) -> String {
    format!("{}.log", id)
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