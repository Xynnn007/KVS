use std::net::SocketAddr;

use tokio::io::{BufWriter, BufReader};
use tokio::io::{WriteHalf, ReadHalf};
use tokio::net::TcpStream;

use crate::err::*;
use crate::protocol::*;

pub struct KvsClient {
    writer : BufWriter<WriteHalf<TcpStream>>,
    reader : BufReader<ReadHalf<TcpStream>>,
}

impl KvsClient {
    pub async fn new(address: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(address).await?;
        let (reader, writer) = tokio::io::split(stream);
        let reader = BufReader::new(reader);
        let writer = BufWriter::new(writer);
        Ok( Self {
            writer,
            reader
        })
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Request::Set(key, value);
        op.write(&mut self.writer).await?;
        match Response::read_from(&mut self.reader).await? {
            Response::Ok => {
                Ok(())
            },
            Response::Error(e) => Err(KvsError::StringError(e)),
            _ => Err(KvsError::StringError("Illegel response".to_string())),
        }
    }

    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let op = Request::Get(key);
        op.write(&mut self.writer).await?;

        match Response::read_from(&mut self.reader).await? {
            Response::Get(v) => Ok(v),
            Response::Error(e) => Err(KvsError::StringError(e)),
            _ => Err(KvsError::StringError("Illegel response".to_string())),
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<()> {
        let op = Request::Remove(key);
        op.write(&mut self.writer).await?;

        match Response::read_from(&mut self.reader).await? {
            Response::Ok => Ok(()),
            Response::Error(e) => Err(KvsError::StringError(e)),
            _ => Err(KvsError::StringError("Illegel response".to_string())),
        }
    }
}