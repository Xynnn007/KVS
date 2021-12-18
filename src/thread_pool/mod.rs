use crate::err::*;

mod naive;

pub use naive::NaiveThreadPool;

pub trait ThreadPool {
    fn new(_threads: u64) -> Result<Self> 
    where Self: Sized;

    fn spawn<F>(&self, job: F) 
        where F:FnOnce() + Send + 'static;
}

