//! Accepted connection IO cannot outlive retained transport shutdown indefinitely.
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tonic::transport::server::Connected;

pub(super) struct RpcIo {
    socket: TcpStream,
    ended: bool,
    stopping: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl RpcIo {
    pub(super) fn new(socket: TcpStream, mut stopped: watch::Receiver<bool>) -> Self {
        Self {
            socket,
            ended: false,
            stopping: Box::pin(async move {
                while !*stopped.borrow_and_update() {
                    if stopped.changed().await.is_err() {
                        return;
                    }
                }
            }),
        }
    }
    fn ended(&mut self, cx: &mut Context<'_>) -> bool {
        if !self.ended {
            self.ended = self.stopping.as_mut().poll(cx).is_ready();
        }
        self.ended
    }
}
impl Connected for RpcIo {
    type ConnectInfo = <TcpStream as Connected>::ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.socket.connect_info()
    }
}
impl AsyncRead for RpcIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.ended(cx) {
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.socket).poll_read(cx, buffer)
        }
    }
}
impl AsyncWrite for RpcIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.ended(cx) {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "application transport stopped",
            )))
        } else {
            Pin::new(&mut self.socket).poll_write(cx, buffer)
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.ended(cx) {
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.socket).poll_flush(cx)
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.ended(cx) {
            Poll::Ready(Ok(()))
        } else {
            Pin::new(&mut self.socket).poll_shutdown(cx)
        }
    }
}
