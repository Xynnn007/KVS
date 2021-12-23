use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::net::TcpStream;

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
        let stream = TcpStream::connect(config.address)?;
        Ok (
            Self {
                stream,
                index: 0,
            }
        )
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = Operation::Set(key, value, self.index);
        let mut writer = BufWriter::new(&self.stream);
        let mut reader = BufReader::new(&self.stream);
        op.to_writer(&mut writer)?;
        writer.flush()?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut reader)? {
            Operation::Ok(_, id) => {
                if id != self.index - 1{
                    Err(KvsError::OperationError)?
                }
                Ok(())
            },
            Operation::Error(_) => {
                Err(KvsError::OperationError)?
            },
            _ => Err(KvsError::OperationError)?
        }
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let op = Operation::Get(key, self.index);
        let mut writer = BufWriter::new(&self.stream);
        let mut reader = BufReader::new(&self.stream);
        op.to_writer(&mut writer)?;
        writer.flush()?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut reader)? {
            Operation::Ok(value, id) => {
                if id != self.index - 1{
                    Err(KvsError::OperationError)?
                }
                Ok(value)
            },
            Operation::Error( _) => {
                Err(KvsError::OperationError)?
            },
            _ => Err(KvsError::OperationError)?
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let op = Operation::Remove(key, self.index);
        let mut writer = BufWriter::new(&self.stream);
        let mut reader = BufReader::new(&self.stream);
        op.to_writer(&mut writer)?;
        writer.flush()?;

        self.index += 1;
        
        match Operation::get_operation_from_reader(&mut reader)? {
            Operation::Ok(_, id) => {
                if id != self.index - 1{
                    Err(KvsError::OperationError)?
                }
                Ok(())
            },
            Operation::Error(u64::MAX) => {
                Err(KvsError::NoEntryError)?
            },
            _ => Err(KvsError::OperationError)?
        }
    }
}