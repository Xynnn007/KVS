use failure::ResultExt;

use crate::engine::*;
use crate::err::*;
use crate::protocol::*;

use std::fs;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::net::{TcpListener};
use std::path::Path;

pub struct KvsServer {
    engine: Box<dyn KvsEngine>,
    listener: TcpListener,
}

pub struct KvsServerConfig<'a> {
    pub address: &'a str,
    pub engine: &'a str,
}

const ENGINE_FLAG_FILE: &str = ".engine_flag";

impl KvsServer {
    pub fn new(config: &KvsServerConfig) -> Result<Self> {
        let address = config.address.parse::<SocketAddr>()
            .context(ErrorKind::ParameterError)?;
        let listener = TcpListener::bind(address)
            .context(ErrorKind::NetworkError)?;

        if !KvsServer::judge_engine_flag(&config.address[..])? {
            Err(ErrorKind::EngineError)?
        }

        Ok(Self {
            engine: match &config.address[..] {
                "kvs" => {
                    Box::new(KvStore::new()?)
                }
                "sled" => {
                    Box::new(SledKvsEngine::new()?)
                }
                _ => {
                    Box::new(KvStore::new()?)
                }
            },
            listener,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        for stream in self.listener.incoming() {
            handle_client(&mut self.engine, &mut stream.context(ErrorKind::NetworkError)?)?;
        }

        Ok(())
    }

    fn judge_engine_flag(name: &str) -> Result<bool> {
        if !Path::new(ENGINE_FLAG_FILE).exists() {
            fs::write(ENGINE_FLAG_FILE, name).context(ErrorKind::IOError)?;
            Ok(true)
        } else {
            let exist = fs::read(ENGINE_FLAG_FILE).context(ErrorKind::IOError)?;
            Ok(exist.eq(name.as_bytes()))
        }
    }
}

fn handle_client(kv: &mut Box<dyn KvsEngine>, stream: &mut TcpStream) -> Result<()> {
    match Operation::get_operation_from_reader(stream)? {
        Operation::Set(k, v, index) => {
            match kv.set(k, v) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        Operation::Get(k, index) => {
            match kv.get(k) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
                Ok(value) => {
                    Operation::Ok(value, index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        Operation::Remove(k, index) => {
            match kv.remove(k) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(stream)?;
                    stream.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        _ => {
            Operation::Error(ErrorKind::OperationError, 0).to_writer(stream)?;
            stream.flush().context(ErrorKind::NetworkError)?;
            Err(ErrorKind::OperationError)?
        }
    }
    Ok(())
}