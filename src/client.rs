use std::io::Write;
use std::net::TcpStream;

use failure::ResultExt;

use crate::err::*;
use crate::protocol::*;

pub struct KvsClient {
    stream: TcpStream,
    index: u64,
}

pub struct KvsClientConfig<'a> {
    pub address: &'a str,
}

impl KvsClient {
    pub fn new(config: &KvsClientConfig) -> Result<Self> {
        let stream = TcpStream::connect(config.address)
            .context(ErrorKind::NetworkError)?;
        Ok (
            Self {
                stream,
                index: 0,
            }
        )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set(key, value, self.index);
        op.to_writer(&mut self.stream)?;
        self.stream.flush().context(ErrorKind::NetworkError)?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut self.stream)? {
            Operation::Ok(_, id) => {
                if id != self.index - 1{
                    Err(ErrorKind::OperationError)?
                }
                Ok(())
            },
            Operation::Error(e, _) => {
                Err(e)?
            },
            _ => Err(ErrorKind::OperationError)?
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let op = Operation::Get(key, self.index);
        op.to_writer(&mut self.stream)?;
        self.stream.flush().context(ErrorKind::NetworkError)?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut self.stream)? {
            Operation::Ok(value, id) => {
                if id != self.index - 1{
                    Err(ErrorKind::OperationError)?
                }
                Ok(value)
            },
            Operation::Error(e, _) => {
                Err(e)?
            },
            _ => Err(ErrorKind::OperationError)?
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let op = Operation::Remove(key, self.index);
        op.to_writer(&mut self.stream)?;
        self.stream.flush().context(ErrorKind::NetworkError)?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut self.stream)? {
            Operation::Ok(_, id) => {
                if id != self.index - 1{
                    Err(ErrorKind::OperationError)?
                }
                Ok(())
            },
            Operation::Error(e, _) => {
                Err(e)?
            },
            _ => Err(ErrorKind::OperationError)?
        }
    }
}