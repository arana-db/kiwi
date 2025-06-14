use async_trait::async_trait;
use std::error::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use crate::{Client, ServerTrait, StreamTrait};
use crate::handle::process_connection;

pub struct TcpStreamWrapper {
    stream: TcpStream,
}

impl TcpStreamWrapper {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl StreamTrait for TcpStreamWrapper {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.read(buf).await
    }
    async fn write(&mut self, data: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream.write(data).await
    }
}

pub struct TcpServer {
    addr: String,
}

impl TcpServer {
    pub fn new(addr: Option<String>) -> Self {
        let addr = addr.unwrap_or_else(|| "127.0.0.1:8080".to_string());
        Self { addr }
    }
}

#[async_trait]
impl ServerTrait for TcpServer {
    async fn start(&self) -> Result<(), Box<dyn Error>> {
        let listener = TcpListener::bind(&self.addr).await?;

        println!("Listening on TCP: {}", self.addr);

        loop {
            let (socket, _) = listener.accept().await?;

            let s = TcpStreamWrapper::new(socket);

            let mut client = Client::new(Box::new(s));

            tokio::spawn(async move {
                process_connection(&mut client).await.unwrap();
            });
        }
    }
}
