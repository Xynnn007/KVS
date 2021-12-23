use std::collections::{HashMap, BTreeMap};
use std::env::current_dir;
use std::io::{SeekFrom, Write, BufWriter};
use std::fs::{File, OpenOptions, self};
use std::path::PathBuf;
use std::io::{Seek, BufReader, Read, self};
use std::sync::{Arc, Mutex, RwLock};

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

#[derive(Clone)]
pub struct KvStore {
    map: Arc<RwLock<BTreeMap<String, Position>>>,
    workdir: Arc<PathBuf>,
    reader: Arc<Mutex<ReadModule>>,
    writer: Arc<Mutex<WriteModule>>,
}

struct ReadModule {
    readers: HashMap<u64, ReadSeeker<File>>,
}

struct WriteModule {
    writer: WriteSeeker<File>,
    index: u64,
    compact_size: u64,
}

impl KvsEngine for KvStore {
    fn set(&self, k: String, v: String) -> Result<()> {
        let e = Entry::Set(k.clone(), v);
        let offset: u64;
        let end: u64;
        let compact_size = {
            let mut w = self.writer.lock().unwrap();
            offset = w.writer.pos as u64;
            serde_json::to_writer(&mut w.writer, &e)?;
            w.writer.flush()?;
            end = w.writer.pos as u64;

            if let Some(old_pos) = self.map.write().unwrap().insert(k, Position{
                log_no: w.index,
                offset,
                size: end - offset,
            }) {
                w.compact_size += old_pos.size;
            }
            w.compact_size
        };
        
        // if size overflowed
        if compact_size > MAX_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&self, k: String) -> Result<Option<String>> {
       if let Some(pos) = self.map.read().unwrap().get(&k) {
            let mut reader = self.reader.lock().unwrap();
            let reader = reader.readers.get_mut(&pos.log_no)
                .ok_or(KvsError::IOError(io::Error::new(std::io::ErrorKind::Other, "get reader failed")))?;
            reader.seek(SeekFrom::Start(pos.offset))?;
            let reader = reader.take(pos.size);

            if let Entry::Set(.., value) = serde_json::from_reader(reader)? { 
                Ok(Some(value))
            } else {
                Err(KvsError::LogError)?
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&self, k: String) -> Result<()> {
        let e = Entry::Remove(k.clone());
        
        {
            let writer = self.writer.clone();
            let mut writer = writer.lock().unwrap();
            serde_json::to_writer(&mut writer.writer, &e)?;
            writer.writer.flush()?;
        }
        
        if let None = self.map.write().unwrap().remove(&k) {
            Err(KvsError::NoEntryError)?
        }

        Ok(())
    }
}

impl KvStore {
    pub fn new() -> Result<Self> {
        KvStore::open(current_dir()?)
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
                        .open(log_file_path)?);
            
            KvStore::init_memory_a_file(&mut map, *it, &mut reader)?;
            readers.insert(*it, reader);
        } 

        let mut log_file = file_path.clone();
        log_file.push(KvStore::get_log_name(index));

        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(current_dir().unwrap().join(log_file.clone()))?);
        let reader = ReadSeeker::new(OpenOptions::new()
                    .read(true)
                    .open(current_dir().unwrap().join(log_file))?);
        readers.insert(index, reader);
        
        Ok(Self {
            map: Arc::new(RwLock::new(map)),
            writer: Arc::new(Mutex::new(WriteModule {
                writer,
                index,
                compact_size: 0,
            })),
            reader: Arc::new(Mutex::new(ReadModule {
                readers,
            })),
            workdir: Arc::new(file_path),
        })
    }

    fn compact(&self) -> Result<()> {
        {
            let mut writer = self.writer.lock().unwrap();
            writer.index += 1;
        }

        self.open_new_log()?;

        {
            let mut writer = self.writer.lock().unwrap();
            for pos in &mut self.map.write().unwrap().values_mut() {
                if let Some(file) = self.reader.lock().unwrap().readers.get_mut(&pos.log_no) {
                    file.seek(SeekFrom::Start(pos.offset))?;
                    let mut reader = io::BufReader::new(file.take(pos.size));
                    let offset = writer.writer.seek(SeekFrom::Current(0))?;
                    io::copy(&mut reader, &mut writer.writer)?;
                    
                    *pos = Position {
                        log_no: writer.index,
                        offset,
                        size: pos.size
                    };

                    file.seek(SeekFrom::End(0))?;
                }
            }
        }
        self.delete_old_logs()?;
        {
            let mut writer = self.writer.lock().unwrap();
            writer.index += 1;
        }
        self.open_new_log()?;
        {
            let mut writer = self.writer.lock().unwrap();
            writer.compact_size = 0;
        }
        Ok(())
    }

    fn delete_old_logs(&self) -> Result<()> {
        let logs = KvStore::get_log_numbers(self.workdir.to_path_buf())?;
        let index = {
            self.writer.lock().unwrap().index
        };
        let mut delete_files = vec![];

        for log in logs.iter() {
            if *log < index {
                let log_name = KvStore::get_log_name(*log);
                {
                    self.reader.lock().unwrap().readers.remove(log);
                }
                let mut log_path= (*self.workdir).clone();
                log_path.push(log_name);
                delete_files.push(log_path);
                
            }
        }

        for f in delete_files {
            fs::remove_file(f.as_path())?;
        }

        Ok(())
    }

    fn open_new_log(&self) -> Result<()> {
        let mut file_path = (*self.workdir).clone();
        {
            let w = self.writer.lock().unwrap();
            file_path.push(KvStore::get_log_name(w.index));
        }

        let writer = WriteSeeker::new(OpenOptions::new()
                    .write(true)
                    .truncate(false)
                    .create(true)
                    .open(file_path.as_path())?);
        let reader = ReadSeeker::new(OpenOptions::new()
                    .read(true)
                    .open(file_path.as_path())?);

        {
            let mut w = self.writer.lock().unwrap();
            w.writer = writer;
            self.reader.lock().unwrap().readers.insert(w.index, reader);
        }

        Ok(())
    }

    fn get_log_name(id: u64) -> String {
        format!("{}.log", id)
    }

    fn init_memory_a_file<R: Read + Seek + Sync>(map: &mut BTreeMap<String, Position>, log_no: u64, reader: &mut ReadSeeker<R>) -> Result<()> {
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