// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    cell::{Cell, RefCell},
    io::{ErrorKind, Read, Write},
    ops::DerefMut,
    rc::Rc,
    sync::Arc,
};

use crate::net::TcpStream;

#[derive(Debug, PartialEq)]
pub enum Error {
    UnknownError,
    BadHandshake,
    PlaintextWriteFailed,
    CiphertextWriteFailed,
    TcpSendFailed,
    TcpRecvFailed,
    InternalReadFailed,
    InvalidCiphertext,
    InvalidServerConfiguration,
    InvalidClientConfiguration,
}

#[derive(Debug)]
struct StreamImpl<TlsConnection> {
    tls_conn: RefCell<TlsConnection>,
    buf: RefCell<Vec<u8>>,
    wrote_close_notify: Cell<bool>,
}

fn write_impl<Data>(
    tls_conn: &mut rustls::ConnectionCommon<Data>, plaintext: &[u8],
    wrote_close_notify: &Cell<bool>,
) -> Result<usize, Error> {
    if wrote_close_notify.get() {
        return Ok(0);
    }

    let n = tls_conn
        .writer()
        .write(plaintext)
        .map_err(|_| Error::PlaintextWriteFailed)?;

    Ok(n)
}

async fn flush_impl<Data, TlsConnection>(
    tcp_stream: &TcpStream, buf: &RefCell<Vec<u8>>, max_send_size: usize,
    tls_stream: &RefCell<TlsConnection>,
) -> Result<usize, Error>
where
    TlsConnection: DerefMut<Target = rustls::ConnectionCommon<Data>>,
{
    let (send_buf, max_send) = {
        let mut borrow_guard = buf.borrow_mut();
        let mut buf = &mut *borrow_guard;
        let tls_conn = &mut *tls_stream.borrow_mut();

        loop {
            if let Err(_err) = tls_conn.write_tls(&mut buf) {
                return Err(Error::CiphertextWriteFailed);
            }

            if !tls_conn.wants_write() {
                break;
            }
        }

        if buf.is_empty() {
            return Ok(0);
        }

        let max_send = max_send_size.min(buf.len());
        let send_buf = std::mem::take(buf);

        (send_buf, max_send)
    };

    let (n, send_buf) = tcp_stream.send_subspan(..max_send, send_buf).await;

    let buf = &mut *buf.borrow_mut();
    *buf = send_buf;

    match n {
        Ok(n) => {
            buf.drain(0..n);
            Ok(n)
        }
        Err(_err) => Err(Error::TcpSendFailed),
    }
}

async fn read_impl<Data, TlsConnection>(
    tcp_stream: &TcpStream, buf: &mut Vec<u8>, tls_stream: &RefCell<TlsConnection>,
    wrote_close_notify: &Cell<bool>,
) -> Result<usize, Error>
where
    TlsConnection: DerefMut<Target = rustls::ConnectionCommon<Data>>,
{
    let mut n = 0;

    {
        let tls_conn = &mut *tls_stream.borrow_mut();

        if !tls_conn.wants_read() {
            let old_len = buf.len();
            match tls_conn.reader().read_to_end(buf) {
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    n += buf.len() - old_len;
                }
                Ok(read) => {
                    n += read;
                }
                Err(_err) => return Err(Error::BadHandshake),
            }
            return Ok(n);
        }
    }

    while n == 0 {
        let bufs = tcp_stream.recv().await.map_err(|_| Error::TcpRecvFailed)?;

        let tls_conn = &mut *tls_stream.borrow_mut();
        for mut b in &bufs {
            while !b.is_empty() {
                tls_conn
                    .read_tls(&mut b)
                    .map_err(|_| Error::InternalReadFailed)?;

                let io_state = tls_conn
                    .process_new_packets()
                    .map_err(|_| Error::InvalidCiphertext)?;

                if !tls_conn.wants_read() {
                    let old_len = buf.len();
                    match tls_conn.reader().read_to_end(buf) {
                        Err(err) if err.kind() == ErrorKind::WouldBlock => {
                            n += buf.len() - old_len;
                        }
                        Ok(read) => {
                            n += read;
                        }
                        Err(_err) => return Err(Error::InternalReadFailed),
                    }
                }

                if io_state.peer_has_closed() {
                    tls_conn.send_close_notify();
                    wrote_close_notify.set(true);
                    return Ok(0);
                }
            }
        }
    }

    Ok(n)
}

fn write_shutdown_impl<Data, TlsConnection>(
    tls_stream: &RefCell<TlsConnection>, wrote_close_notify: &Cell<bool>,
) where
    TlsConnection: DerefMut<Target = rustls::ConnectionCommon<Data>>,
{
    if wrote_close_notify.get() {
        return;
    }

    tls_stream.borrow_mut().send_close_notify();
    wrote_close_notify.set(true);
}

#[derive(Clone, Debug)]
pub struct TlsStream {
    tcp_stream: TcpStream,
    stream_impl: Rc<StreamImpl<rustls::ServerConnection>>,
}

impl TlsStream {
    pub fn write(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        write_impl(tls_conn, plaintext, &self.stream_impl.wrote_close_notify)
    }

    pub fn write_shutdown(&self) {
        write_shutdown_impl(&self.stream_impl.tls_conn, &self.stream_impl.wrote_close_notify);
    }

    pub async fn flush(&self, max_send_size: usize) -> Result<usize, Error> {
        flush_impl(
            &self.tcp_stream,
            &self.stream_impl.buf,
            max_send_size,
            &self.stream_impl.tls_conn,
        )
        .await
    }

    pub async fn read(&self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        read_impl(
            &self.tcp_stream,
            buf,
            &self.stream_impl.tls_conn,
            &self.stream_impl.wrote_close_notify,
        )
        .await
    }
}

#[derive(Clone, Debug)]
pub struct TlsClient {
    tcp_stream: TcpStream,
    stream_impl: Rc<StreamImpl<rustls::ClientConnection>>,
}

impl TlsClient {
    pub fn write(&self, plaintext: &[u8]) -> Result<usize, Error> {
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        write_impl(tls_conn, plaintext, &self.stream_impl.wrote_close_notify)
    }

    pub async fn flush(&self, max_send_size: usize) -> Result<usize, Error> {
        flush_impl(
            &self.tcp_stream,
            &self.stream_impl.buf,
            max_send_size,
            &self.stream_impl.tls_conn,
        )
        .await
    }

    pub async fn read(&self, buf: &mut Vec<u8>) -> Result<usize, Error> {
        read_impl(
            &self.tcp_stream,
            buf,
            &self.stream_impl.tls_conn,
            &self.stream_impl.wrote_close_notify,
        )
        .await
    }

    pub fn write_shutdown(&self) {
        write_shutdown_impl(&self.stream_impl.tls_conn, &self.stream_impl.wrote_close_notify);
    }
}

pub async fn server_handshake(
    stream: TcpStream, config: Arc<rustls::ServerConfig>,
) -> Result<TlsStream, Error> {
    let config =
        rustls::ServerConnection::new(config).map_err(|_| Error::InvalidServerConfiguration)?;

    let tls_stream = TlsStream {
        tcp_stream: stream,
        stream_impl: Rc::new(StreamImpl {
            tls_conn: RefCell::new(config),
            buf: RefCell::new(Vec::new()),
            wrote_close_notify: Cell::new(false),
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
            tls_conn
                .borrow_mut()
                .write_tls(&mut buf)
                .map_err(|_| Error::CiphertextWriteFailed)?;

            let (n, send_buf) = tls_stream.tcp_stream.send(buf).await;
            if n.is_err() {
                return Err(Error::TcpSendFailed);
            }

            buf = send_buf;
            buf.clear();
        }

        if is_handshaking && wants_read {
            let bufs = tls_stream
                .tcp_stream
                .recv()
                .await
                .map_err(|_| Error::TcpRecvFailed)?;

            if bufs.is_empty() {
                return Err(Error::BadHandshake);
            }

            let mut tls_conn = tls_conn.borrow_mut();

            for mut b in &bufs {
                tls_conn
                    .read_tls(&mut b)
                    .map_err(|_| Error::InternalReadFailed)?;
            }

            tls_conn
                .process_new_packets()
                .map_err(|_| Error::BadHandshake)?;
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
            tls_conn: RefCell::new(
                rustls::ClientConnection::new(config, server_name)
                    .map_err(|_| Error::InvalidClientConfiguration)?,
            ),
            buf: RefCell::new(Vec::new()),
            wrote_close_notify: Cell::new(false),
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
            tls_conn
                .borrow_mut()
                .write_tls(&mut buf)
                .map_err(|_| Error::CiphertextWriteFailed)?;

            let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
            if n.is_err() {
                return Err(Error::TcpSendFailed);
            }
            buf = send_buf;
            buf.clear();
        }

        if is_handshaking && wants_read {
            let bufs = tls_client
                .tcp_stream
                .recv()
                .await
                .map_err(|_| Error::TcpRecvFailed)?;

            if bufs.is_empty() {
                return Err(Error::BadHandshake);
            }

            let mut tls_conn = tls_conn.borrow_mut();

            for mut b in &bufs {
                tls_conn
                    .read_tls(&mut b)
                    .map_err(|_| Error::InternalReadFailed)?;
            }

            tls_conn
                .process_new_packets()
                .map_err(|_| Error::BadHandshake)?;
        }

        if !is_handshaking {
            break;
        }
    }

    let wants_write = { tls_conn.borrow().wants_write() };
    if wants_write {
        tls_conn
            .borrow_mut()
            .write_tls(&mut buf)
            .map_err(|_| Error::CiphertextWriteFailed)?;

        let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
        if n.is_err() {
            return Err(Error::TcpSendFailed);
        }

        buf = send_buf;
        buf.clear();
    }

    Ok(tls_client)
}
