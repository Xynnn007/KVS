use crate::err::*;

mod naive;
mod shared_queue;

pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;
pub trait ThreadPool {
    fn new(_threads: u64) -> Result<Self> 
    where Self: Sized;

    fn spawn<F>(&self, job: F) 
    where F:FnOnce() + Send + 'static;
}

