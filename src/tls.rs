// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    cell::{Cell, RefCell},
    io::{ErrorKind, Read, Write},
    rc::Rc,
    sync::Arc,
};

use crate::net::TcpStream;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Tls(rustls::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<nix::Error> for Error {
    fn from(err: nix::Error) -> Self {
        Error::Io(err.into())
    }
}

impl From<rustls::Error> for Error {
    fn from(err: rustls::Error) -> Self {
        Error::Tls(err)
    }
}

struct StreamImpl<TlsConnection> {
    tls_conn: RefCell<TlsConnection>,
    buf: RefCell<Vec<u8>>,
    send_pending: Cell<bool>,
    recv_pending: Cell<bool>,
}

struct PendingDropGuard<'a> {
    pending: &'a Cell<bool>,
}

impl<'a> PendingDropGuard<'a> {
    fn new(pending: &'a Cell<bool>) -> Self {
        pending.set(true);
        Self { pending }
    }
}

impl Drop for PendingDropGuard<'_> {
    fn drop(&mut self) {
        self.pending.set(false);
    }
}

pub struct TlsStream {
    tcp_stream: TcpStream,
    stream_impl: Rc<StreamImpl<rustls::ServerConnection>>,
}

impl TlsStream {
    pub fn write_tls(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        let n = tls_conn.writer().write(plaintext)?;
        Ok(n)
    }

    pub async fn flush_tls(&self, max_send_size: usize) -> Result<usize, Error> {
        let mut buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());
        {
            let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
            while tls_conn.wants_write() {
                tls_conn.write_tls(&mut buf)?;
            }
        }

        if buf.is_empty() {
            return Ok(0);
        }

        let (n, mut buf) = self
            .tcp_stream
            .send_subspan(..max_send_size.min(buf.len()), buf)
            .await;
        let n = n?;

        buf.drain(0..n);
        *self.stream_impl.buf.borrow_mut() = buf;

        Ok(n)
    }

    pub async fn read_tls(&self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        {
            let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();

            let has_pending_plaintext = !tls_conn.wants_read();
            if has_pending_plaintext {
                let mut n = 0;
                let old_len = buf.len();
                match tls_conn.reader().read_to_end(buf) {
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        n += buf.len() - old_len;
                    }
                    Ok(read) => {
                        n += read;
                    }
                    Err(err) => return Err(err.into()),
                }
                return Ok(n);
            }
        }

        let bufs = self.tcp_stream.recv().await?;

        let mut n = 0;
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();

        for mut b in &bufs {
            while !b.is_empty() {
                tls_conn.read_tls(&mut b)?;
                tls_conn.process_new_packets()?;
                if !tls_conn.wants_read() {
                    let old_len = buf.len();
                    match tls_conn.reader().read_to_end(buf) {
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            n += buf.len() - old_len;
                        }
                        Ok(read) => {
                            n += read;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
            }
        }

        Ok(n)
    }

    pub async fn send(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let _guard = PendingDropGuard::new(&self.stream_impl.send_pending);

        let mut buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());

        let tls_conn = &self.stream_impl.tls_conn;
        tls_conn.borrow_mut().writer().write_all(plaintext)?;
        tls_conn.borrow_mut().write_tls(&mut buf)?;

        let (n, send_buf) = self.tcp_stream.send(buf).await;
        buf = send_buf;
        buf.clear();
        *self.stream_impl.buf.borrow_mut() = buf;

        Ok(n?)
    }

    pub async fn recv(&self, plaintext: &mut [u8]) -> Result<usize, Error> {
        let _guard = PendingDropGuard::new(&self.stream_impl.recv_pending);

        let tls_conn = &self.stream_impl.tls_conn;

        let has_pending_plaintext = !tls_conn.borrow().wants_read();
        if has_pending_plaintext {
            return tls_conn
                .borrow_mut()
                .reader()
                .read(plaintext)
                .map_err(Error::from);
        }

        let bufs = self.tcp_stream.recv().await?;

        let conn = &mut *tls_conn.borrow_mut();
        for mut b in &bufs {
            conn.read_tls(&mut b)?;
        }

        conn.process_new_packets()?;
        conn.reader().read(plaintext).map_err(Error::from)
    }

    #[allow(clippy::unused_async)]
    pub async fn shutdown(&self) -> Result<(), Error> {
        Ok(())
    }
}

pub struct TlsClient {
    tcp_stream: TcpStream,
    stream_impl: Rc<StreamImpl<rustls::ClientConnection>>,
}

impl TlsClient {
    pub fn write_tls(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        let n = tls_conn.writer().write(plaintext)?;
        Ok(n)
    }

    pub async fn flush_tls(&self, max_send_size: usize) -> Result<usize, Error> {
        let mut buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());
        {
            let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
            while tls_conn.wants_write() {
                tls_conn.write_tls(&mut buf)?;
            }
        }

        if buf.is_empty() {
            return Ok(0);
        }

        let (n, mut buf) = self
            .tcp_stream
            .send_subspan(..max_send_size.min(buf.len()), buf)
            .await;
        let n = n?;

        buf.drain(0..n);
        *self.stream_impl.buf.borrow_mut() = buf;

        Ok(n)
    }

    pub async fn read_tls(&self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        {
            let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();

            let has_pending_plaintext = !tls_conn.wants_read();
            if has_pending_plaintext {
                let mut n = 0;
                let old_len = buf.len();
                match tls_conn.reader().read_to_end(buf) {
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {
                        n += buf.len() - old_len;
                    }
                    Ok(read) => {
                        n += read;
                    }
                    Err(err) => return Err(err.into()),
                }
                return Ok(n);
            }
        }

        let bufs = self.tcp_stream.recv().await?;

        let mut n = 0;
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();

        for mut b in &bufs {
            while !b.is_empty() {
                tls_conn.read_tls(&mut b)?;
                tls_conn.process_new_packets()?;
                if !tls_conn.wants_read() {
                    let old_len = buf.len();
                    match tls_conn.reader().read_to_end(buf) {
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            n += buf.len() - old_len;
                        }
                        Ok(read) => {
                            n += read;
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
            }
        }

        Ok(n)
    }

    pub async fn send(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let _guard = PendingDropGuard::new(&self.stream_impl.send_pending);

        let mut buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());

        let tls_conn = &self.stream_impl.tls_conn;
        tls_conn.borrow_mut().writer().write_all(plaintext)?;
        tls_conn.borrow_mut().write_tls(&mut buf)?;

        let (n, send_buf) = self.tcp_stream.send(buf).await;
        buf = send_buf;
        buf.clear();
        *self.stream_impl.buf.borrow_mut() = buf;

        Ok(n?)
    }

    pub async fn recv(&self, plaintext: &mut [u8]) -> Result<usize, Error> {
        let _guard = PendingDropGuard::new(&self.stream_impl.recv_pending);

        let tls_conn = &self.stream_impl.tls_conn;

        let has_pending_plaintext = !tls_conn.borrow().wants_read();
        if has_pending_plaintext {
            return tls_conn
                .borrow_mut()
                .reader()
                .read(plaintext)
                .map_err(Error::from);
        }

        let bufs = self.tcp_stream.recv().await?;

        let conn = &mut *tls_conn.borrow_mut();
        for mut b in &bufs {
            conn.read_tls(&mut b)?;
        }

        conn.process_new_packets()?;
        conn.reader().read(plaintext).map_err(Error::from)
    }
}

pub async fn server_handshake(
    stream: TcpStream, config: Arc<rustls::ServerConfig>,
) -> Result<TlsStream, Error> {
    let config = rustls::ServerConnection::new(config)?;

    let tls_stream = TlsStream {
        tcp_stream: stream,
        stream_impl: Rc::new(StreamImpl {
            tls_conn: RefCell::new(config),
            buf: RefCell::new(Vec::new()),
            send_pending: Cell::new(false),
            recv_pending: Cell::new(false),
        }),
    };

    let tls_conn = &tls_stream.stream_impl.tls_conn;
    let mut buf = std::mem::take(&mut *tls_stream.stream_impl.buf.borrow_mut());

    loop {
        let (is_handshaking, wants_write, wants_read) = (
            tls_conn.borrow().is_handshaking(),
            tls_conn.borrow().wants_write(),
            tls_conn.borrow().wants_read(),
        );

        if wants_write {
            tls_conn.borrow_mut().write_tls(&mut buf)?;
            let (n, send_buf) = tls_stream.tcp_stream.send(buf).await;
            if let Err(err) = n {
                return Err(err.into());
            }
            buf = send_buf;
            buf.clear();
        }

        if is_handshaking && wants_read {
            let bufs = tls_stream.tcp_stream.recv().await?;
            if bufs.is_empty() {
                return Err(Error::Tls(rustls::Error::HandshakeNotComplete));
            }

            for mut b in &bufs {
                tls_conn.borrow_mut().read_tls(&mut b)?;
            }
            tls_conn.borrow_mut().process_new_packets()?;
        }

        if !is_handshaking {
            break;
        }
    }

    *tls_stream.stream_impl.buf.borrow_mut() = buf;

    Ok(tls_stream)
}

pub async fn client_handshake(
    stream: TcpStream, config: Arc<rustls::ClientConfig>,
    server_name: rustls_pki_types::ServerName<'static>,
) -> Result<TlsClient, Error> {
    let tls_client = TlsClient {
        tcp_stream: stream,
        stream_impl: Rc::new(StreamImpl {
            tls_conn: RefCell::new(rustls::ClientConnection::new(config, server_name)?),
            buf: RefCell::new(Vec::new()),
            send_pending: Cell::new(false),
            recv_pending: Cell::new(false),
        }),
    };

    let mut buf = std::mem::take(&mut *tls_client.stream_impl.buf.borrow_mut());
    let tls_conn = &tls_client.stream_impl.tls_conn;
    loop {
        let (is_handshaking, wants_write, wants_read) = (
            tls_conn.borrow().is_handshaking(),
            tls_conn.borrow().wants_write(),
            tls_conn.borrow().wants_read(),
        );

        if wants_write {
            tls_conn.borrow_mut().write_tls(&mut buf)?;
            let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
            if let Err(err) = n {
                return Err(err.into());
            }
            buf = send_buf;
            buf.clear();
        }

        if is_handshaking && wants_read {
            let bufs = tls_client.tcp_stream.recv().await?;
            if bufs.is_empty() {
                return Err(Error::Tls(rustls::Error::HandshakeNotComplete));
            }

            for mut b in &bufs {
                tls_conn.borrow_mut().read_tls(&mut b)?;
            }
            tls_conn.borrow_mut().process_new_packets()?;
        }

        if !is_handshaking {
            break;
        }
    }

    let wants_write = { tls_conn.borrow().wants_write() };
    if wants_write {
        tls_conn.borrow_mut().write_tls(&mut buf)?;
        let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
        if let Err(err) = n {
            return Err(err.into());
        }
        buf = send_buf;
        buf.clear();
    }

    Ok(tls_client)
}
