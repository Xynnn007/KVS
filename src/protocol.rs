use std::io::{Write, Read};

use crate::err::*;

use serde::{Serialize, Deserialize};
use tokio::io::{ AsyncWrite, AsyncWriteExt};
use tokio::io::{ AsyncRead, AsyncReadExt};

#[derive(Serialize, Deserialize)]
pub enum Request {
    Set(String, String),
    Get(String),
    Remove(String),
}

#[derive(Serialize, Deserialize)]
pub enum Response {
    Get(Option<String>), 
    Ok,
    Error(String),
}

impl Request {
    pub async fn write(&self, mut w: impl AsyncWrite + Unpin + Send) -> Result<()> {
        let data = serde_json::to_string(self)?;
        w.write(data.as_bytes()).await?;
        w.flush().await?;
        Ok(())
    }

    pub fn read_from(r: impl Read) -> Result<Self> {
        let mut de = serde_json::Deserializer::from_reader(r);
        Ok(Request::deserialize(&mut de)?)
    }
}

impl Response {
    pub async fn read_from(mut r: impl AsyncRead + Unpin + Send) -> Result<Self> {
        let mut data = String::new();
        r.read_to_string(&mut data).await?;
        Ok(serde_json::from_str(&data)?)
    }

    pub fn write(&self, mut w: impl Write) -> Result<()> {
        let data = serde_json::to_string(self)?;
        w.write(data.as_bytes())?;
        w.flush()?;
        Ok(())
    }
}