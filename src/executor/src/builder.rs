use crate::CmdExecutor;

pub struct CmdExecutorBuilder {
    worker_count: usize,
}

impl CmdExecutorBuilder {
    pub fn new() -> Self {
        Self {
            worker_count: num_cpus::get(),
        }
    }

    pub fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn build(self) -> CmdExecutor {
        CmdExecutor::new(self.worker_count)
    }
}
