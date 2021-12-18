use std::thread;

use super::ThreadPool;

use crate::err::*;
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u64) -> Result<Self> 
    where Self: Sized
    {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F) 
    where F:FnOnce() + Send + 'static 
    {
        thread::spawn(job);
    }
}