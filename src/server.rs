use log::error;
use log::warn;

use crate::engine::*;
use crate::err::*;
use crate::protocol::*;
use crate::thread_pool::ThreadPool;

use std::io::BufReader;
use std::io::BufWriter;
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
    match Request::read_from(&mut reader)? {
        Request::Set(k, v) => {
            match engine.set(k, v) {
                Err(e) => {
                    error!("{}", e);
                    Response::Error(e.to_string()).write(&mut writer)?;
                }
                _ => {
                    Response::Ok.write(&mut writer)?;
                }
            }
        },
        Request::Get(k) => {
            match engine.get(k) {
                Err(e) => {
                    error!("{}", e);
                    Response::Error(e.to_string()).write(&mut writer)?;
                }
                Ok(value) => {
                    Response::Get(value).write(&mut writer)?;
                }
            }
        },
        Request::Remove(k) => {
            match engine.remove(k) {
                Err(e) => {
                    Response::Error(e.to_string()).write(&mut writer)?;
                    match e {
                        KvsError::NoEntryError => {
                            warn!("{}", e);
                        },
                        _ => {
                            error!("{}", e);
                        },
                    }
                }
                _ => {
                    Response::Ok.write(&mut writer)?;
                }
            }
        },
    }
    Ok(())
}