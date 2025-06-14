use async_trait::async_trait;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use crate::handle::process_connection;
use crate::{Client, ServerTrait, StreamTrait};

pub struct UnixStreamWrapper {
    stream: UnixStream,
}

impl UnixStreamWrapper {
    pub fn new(stream: UnixStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl StreamTrait for UnixStreamWrapper {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }
    async fn write(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

pub struct UnixServer {
    path: String,
}

impl UnixServer {
    pub fn new(path: Option<String>) -> Self {
        let path = path.unwrap_or_else(|| "/tmp/sagedb.sock".to_string());
        Self { path }
    }
}

#[async_trait]
impl ServerTrait for UnixServer {
    async fn start(&self) -> Result<(), Box<dyn Error>> {
        let _ = std::fs::remove_file(&self.path);
        let listener = UnixListener::bind(&self.path)?;
        println!("Listening on Unix Socket: {}", self.path);

        loop {
            let (socket, _) = listener.accept().await?;

            let s = UnixStreamWrapper::new(socket);

            let mut client = Client::new(Box::new(s));

            tokio::spawn(async move {
                process_connection(&mut client).await.unwrap()
            });
        }
    }
}
