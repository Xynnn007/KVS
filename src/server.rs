use log::error;
use log::warn;

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
        let address = address.parse::<SocketAddr>()?;
        let listener = TcpListener::bind(address)?;
        
        Ok(Self {
            engine,
            listener,
            thread_pool
        })
    }

    pub fn run(&self) -> Result<()> {
        for stream in self.listener.incoming() {
            match stream {
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
                    error!("{}", e);
                    Operation::Error(index).to_writer(&mut writer)?;
                    writer.flush()?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(&mut writer)?;
                    writer.flush()?;
                }
            }
        },
        Operation::Get(k, index) => {
            match engine.get(k) {
                Err(e) => {
                    error!("{}", e);
                    Operation::Error(index).to_writer(&mut writer)?;
                    writer.flush()?;
                }
                Ok(value) => {
                    Operation::Ok(value, index).to_writer(&mut writer)?;
                    writer.flush()?;
                }
            }
        },
        Operation::Remove(k, index) => {
            match engine.remove(k) {
                Err(e) => {
                    match e {
                        KvsError::NoEntryError => {
                            Operation::Error(u64::MAX).to_writer(&mut writer)?;
                            warn!("{}", e);
                        },
                        _ => {
                            Operation::Error(index).to_writer(&mut writer)?;
                            error!("{}", e);
                        },
                    }
                    writer.flush()?;
                }
                _ => {
                    Operation::Ok(Some(String::new()), index).to_writer(&mut writer)?;
                    writer.flush()?;
                }
            }
        },
        _ => {
            error!("get a wrong type operation");
            Operation::Error(0).to_writer(&mut writer)?;
            writer.flush()?;
            Err(KvsError::OperationError)?
        }
    }
    Ok(())
}