
use failure::ResultExt;
use rayon::ThreadPoolBuilder;

use super::ThreadPool;
use crate::err::*;

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool{
    fn new(_threads: u64) -> Result<Self> 
    where Self: Sized {
        let pool = ThreadPoolBuilder::new()
            .num_threads(_threads as usize)
            .build()
            .context(ErrorKind::RayonError)?;
        Ok(Self {
            pool, 
        })
    }

    fn spawn<F>(&self, job: F) 
    where F:FnOnce() + Send + 'static {
        self.pool.spawn(job);
    }
}