use failure::ResultExt;

use crate::engine::*;
use crate::err::*;
use crate::protocol::*;

use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::net::{TcpListener};

pub struct KvsServer<E: KvsEngine> {
    engine: E, 
    listener: TcpListener,
}

pub struct KvsServerConfig<'a> {
    pub address: &'a str,
    pub engine: &'a str,
}

impl<E: KvsEngine> KvsServer<E> {
    pub fn new(address: &str, engine: E) -> Result<Self> {
        let address = address.parse::<SocketAddr>()
            .context(ErrorKind::ParameterError)?;
        let listener = TcpListener::bind(address)
            .context(ErrorKind::NetworkError)?;

        Ok(Self {
            engine,
            listener,
        })
    }

    pub fn run(&mut self) -> Result<()> {
        for stream in self.listener.incoming() {
            self.handle_client(stream.context(ErrorKind::NetworkError)?)?;
        }

        Ok(())
    }

    fn handle_client(&self, mut stream: TcpStream) -> Result<()> {
        match Operation::get_operation_from_reader(&mut stream)? {
            Operation::Set(k, v, index) => {
                match self.engine.set(k, v) {
                    Err(e) => {
                        Operation::Error(e.kind(), index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                    _ => {
                        Operation::Ok(Some(String::new()), index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                }
            },
            Operation::Get(k, index) => {
                match self.engine.get(k) {
                    Err(e) => {
                        Operation::Error(e.kind(), index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                    Ok(value) => {
                        Operation::Ok(value, index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                }
            },
            Operation::Remove(k, index) => {
                match self.engine.remove(k) {
                    Err(e) => {
                        Operation::Error(e.kind(), index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                    _ => {
                        Operation::Ok(Some(String::new()), index).to_writer(&mut stream)?;
                        stream.flush().context(ErrorKind::NetworkError)?;
                    }
                }
            },
            _ => {
                Operation::Error(ErrorKind::OperationError, 0).to_writer(&mut stream)?;
                stream.flush().context(ErrorKind::NetworkError)?;
                Err(ErrorKind::OperationError)?
            }
        }
        Ok(())
    }
}