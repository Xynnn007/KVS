use failure::ResultExt;
use log::error;

use crate::engine::*;
use crate::err::*;
use crate::protocol::*;
use crate::thread_pool::ThreadPool;

use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::net::TcpListener;

pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    engine: E, 
    thread_pool: T,
    listener: TcpListener,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    pub fn new(address: &str, engine: E, thread_pool: T) -> Result<Self> {
        let address = address.parse::<SocketAddr>()
            .context(ErrorKind::ParameterError)?;
        let listener = TcpListener::bind(address)
            .context(ErrorKind::NetworkError)?;
        
        Ok(Self {
            engine,
            listener,
            thread_pool
        })
    }

    pub fn run(&self) -> Result<()> {
        for stream in self.listener.incoming() {
            match stream.context(ErrorKind::NetworkError) {
                Ok(stream) => {
                    let engine = self.engine.clone();
                    self.thread_pool.spawn(move || {
                        match handle_client(engine, stream) {
                            Ok(()) => {},
                            Err(e) => error!("stream handle error {}.", e),
                        }
                    })
                },
                Err(e) => error!("stream handle error {}.", e),
            }
        }

        Ok(())
    }
}

fn handle_client<E: KvsEngine>(engine: E, stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    match Operation::get_operation_from_reader(&mut reader)? {
        Operation::Set(k, v, index) => {
            match engine.set(k, v) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        Operation::Get(k, index) => {
            match engine.get(k) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
                Ok(value) => {
                    Operation::Ok(value, index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        Operation::Remove(k, index) => {
            match engine.remove(k) {
                Err(e) => {
                    Operation::Error(e.kind(), index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(&mut writer)?;
                    writer.flush().context(ErrorKind::NetworkError)?;
                }
            }
        },
        _ => {
            Operation::Error(ErrorKind::OperationError, 0).to_writer(&mut writer)?;
            writer.flush().context(ErrorKind::NetworkError)?;
            Err(ErrorKind::OperationError)?
        }
    }
    Ok(())
}