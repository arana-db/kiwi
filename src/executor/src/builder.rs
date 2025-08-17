use crate::CmdExecutor;

pub struct CmdExecutorBuilder {
    worker_count: usize,
    channel_size: usize,
}

impl CmdExecutorBuilder {
    pub fn new() -> Self {
        Self {
            worker_count: num_cpus::get(),
            channel_size: 1000,
        }
    }

    pub fn worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count;
        self
    }

    pub fn channel_size(mut self, channel_size: usize) -> Self {
        self.channel_size = channel_size;
        self
    }

    pub fn build(self) -> CmdExecutor {
        CmdExecutor::new(self.worker_count, self.channel_size)
    }
}
