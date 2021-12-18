use failure::ResultExt;

use crate::engine::*;
use crate::err::*;
use crate::protocol::*;

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::net::{TcpListener};

pub struct KvsServer {
    engine: Box<dyn KvsEngine>,
    listener: TcpListener,
}

pub struct KvsServerConfig<'a> {
    pub address: &'a str,
    pub engine: &'a str,
}

impl KvsServer {
    pub fn new(config: &KvsServerConfig) -> Result<Self> {
        let address = config.address.parse::<SocketAddr>()
            .context(ErrorKind::ParameterError)?;
        let listener = TcpListener::bind(address)
            .context(ErrorKind::NetworkError)?;

        Ok(Self {
            engine: match config.engine {
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