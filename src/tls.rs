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
    wrote_close_notify: Cell<bool>,
}

fn write_impl<Data>(
    tls_conn: &mut rustls::ConnectionCommon<Data>, plaintext: &[u8],
    wrote_close_notify: &Cell<bool>,
) -> Result<usize, Error> {
    if wrote_close_notify.get() {
        return Ok(0);
    }
    let n = tls_conn.writer().write(plaintext).map_err(Error::from)?;
    Ok(n)
}

async fn flush_impl<Data, TlsConnection>(
    tcp_stream: &TcpStream, mut buf: Vec<u8>, max_send_size: usize,
    tls_stream: &RefCell<TlsConnection>,
) -> (Result<usize, Error>, Vec<u8>)
where
    TlsConnection: DerefMut<Target = rustls::ConnectionCommon<Data>>,
{
    {
        let tls_conn = &mut *tls_stream.borrow_mut();
        loop {
            if let Err(err) = tls_conn.write_tls(&mut buf) {
                return (Err(err.into()), buf);
            }

            if !tls_conn.wants_write() {
                break;
            }
        }
    }

    if buf.is_empty() {
        return (Ok(0), buf);
    }

    let (n, mut buf) = tcp_stream
        .send_subspan(..max_send_size.min(buf.len()), buf)
        .await;

    match n {
        Ok(n) => {
            buf.drain(0..n);
            (Ok(n), buf)
        }
        Err(err) => (Err(err.into()), buf),
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
                Err(err) => return Err(err.into()),
            }
            return Ok(n);
        }
    }

    while n == 0 {
        let bufs = tcp_stream.recv().await?;

        let tls_conn = &mut *tls_stream.borrow_mut();

        for mut b in &bufs {
            while !b.is_empty() {
                tls_conn.read_tls(&mut b)?;
                let io_state = tls_conn.process_new_packets()?;

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

#[derive(Clone)]
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
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        tls_conn.send_close_notify();
        self.stream_impl.wrote_close_notify.set(true);
    }

    pub async fn flush(&self, max_send_size: usize) -> Result<usize, Error> {
        let buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());
        let (n, send_buf) =
            flush_impl(&self.tcp_stream, buf, max_send_size, &self.stream_impl.tls_conn).await;
        *self.stream_impl.buf.borrow_mut() = send_buf;
        n
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

#[derive(Clone)]
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
        let buf = std::mem::take(&mut *self.stream_impl.buf.borrow_mut());
        let (n, send_buf) =
            flush_impl(&self.tcp_stream, buf, max_send_size, &self.stream_impl.tls_conn).await;
        *self.stream_impl.buf.borrow_mut() = send_buf;
        n
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
        let tls_conn = &mut *self.stream_impl.tls_conn.borrow_mut();
        tls_conn.send_close_notify();
        self.stream_impl.wrote_close_notify.set(true);
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
