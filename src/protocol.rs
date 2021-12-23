use std::io;

use serde::{Serialize, Deserialize};

use crate::err::*;

#[derive(Serialize, Deserialize)]
pub enum Operation {
    Set(String, String, u64),
    Get(String, u64),
    Remove(String, u64),
    Ok(Option<String>, u64),
    Error(u64),
}

impl Operation {
    pub fn to_writer<W>(&self, w: &mut W) -> Result<()> 
    where W: io::Write
    {
        serde_json::to_writer(w, self)?;
        
        Ok(())
    }

    pub fn get_operation_from_reader<R>(r: &mut R) -> Result<Self>
    where R: io::Read
    {
        let mut de = serde_json::Deserializer::from_reader(r);
        Ok(Operation::deserialize(&mut de)?)
    }
}