use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::mpsc::{Sender, Receiver, channel};
use std::sync::atomic::Ordering::SeqCst;
use log::error;

use super::ThreadPool;

use crate::err::*;


struct Task {
    inner : Box<dyn FnOnce() + Send + 'static>,
} 

impl Task {
    fn new(inner: Box<dyn FnOnce() + Send + 'static>) -> Self {
        Self {
            inner
        }
    }
    
    fn call(self) {
        let f = |f: Box<dyn FnOnce() + Send + 'static>| {f()};
        f(self.inner);
    }
}
pub struct SharedQueueThreadPool {
    task_sender: Sender<Task>,
    data: Arc<SharedQueueData>
}

struct SharedQueueData {
    task_receiver: Mutex<Receiver<Task>>,
    threads_num: u64,
    threads_alive: AtomicU64,
}

struct Guard {
    data: Arc<SharedQueueData>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u64) -> Result<Self> 
    where Self: Sized
    {
        let threads_num = threads;
        let threads_alive = AtomicU64::new(0);
        let (task_sender, task_receiver) = channel::<Task>();
        let task_receiver = Mutex::new(task_receiver);

        let data = Arc::new(SharedQueueData {
            threads_num,
            task_receiver,
            threads_alive,
        });

        let pool = SharedQueueThreadPool {
            data,
            task_sender,
        };

        for _ in 0..threads {
            pool.new_worker();
        }

        Ok(pool)
    }

    fn spawn<H>(&self, job: H) 
    where H: FnOnce() + Send + 'static {
        if let Err(e) = self.task_sender.send(Task::new(Box::new(job))) {
            error!("error occurs {}", e);
        };
    }
}

impl Guard {
    fn new(data: Arc<SharedQueueData>) -> Self {
        Self {
            data,
        }
    }

    fn work(&self) {
        // do nothing to avoid compiler error
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if thread::panicking() {
            self.data.threads_alive.fetch_sub(1, SeqCst);
        }

        while self.data.threads_alive.load(SeqCst) < self.data.threads_num {
            new_thread(self.data.clone());
        }
    }
}

impl SharedQueueThreadPool {
    fn new_worker(&self) {
        new_thread(self.data.clone());
    }
}

fn new_thread(data: Arc<SharedQueueData>) {
    thread::spawn(move || {
        data.threads_alive.fetch_add(1, SeqCst);
        let guard = Guard::new(data.clone());
        guard.work();
        'exit:      
        loop {
            let task;
            {
                task = data.task_receiver.lock().unwrap().recv();
            }

            let task = match task {
                Ok(t) => t,
                Err(_) => break 'exit,
            };

            task.call();
        }
    });
}