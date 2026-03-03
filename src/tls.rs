// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    cell::{Cell, RefCell},
    io::{Read, Write},
    rc::Rc,
    sync::Arc,
};

use crate::tcp;

struct StreamImpl<ConnectionType> {
    tls_conn: RefCell<ConnectionType>,
    buf: RefCell<Vec<u8>>,
    send_pending: Cell<bool>,
    recv_pending: Cell<bool>,
}

pub struct Stream {
    tcp_stream: tcp::Stream,
    stream_impl: Rc<StreamImpl<rustls::ServerConnection>>,
}

pub struct Client {
    tcp_stream: tcp::Stream,
    stream_impl: Rc<StreamImpl<rustls::ClientConnection>>,
}

pub async fn server_handshake(
    stream: tcp::Stream, config: Arc<rustls::ServerConfig>,
) -> Result<Stream, std::io::Error> {
    let config = rustls::ServerConnection::new(config).unwrap();

    let tls_stream = Stream {
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
            for mut b in &bufs {
                tls_conn.borrow_mut().read_tls(&mut b)?;
            }
            tls_conn.borrow_mut().process_new_packets().unwrap();
        }

        if !is_handshaking {
            break;
        }
    }

    *tls_stream.stream_impl.buf.borrow_mut() = buf;

    Ok(tls_stream)
}

impl Stream {
    pub async fn send(&self, plaintext: &[u8]) -> Result<usize, std::io::Error> {
        self.stream_impl.send_pending.set(true);

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

    pub async fn recv(&self, plaintext: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream_impl.recv_pending.set(true);
        let tls_conn = &self.stream_impl.tls_conn;
        if !tls_conn.borrow().wants_read() {
            return tls_conn.borrow_mut().reader().read(plaintext);
        }

        let bufs = self.tcp_stream.recv().await?;

        for mut b in &bufs {
            tls_conn.borrow_mut().read_tls(&mut b)?;
        }
        tls_conn.borrow_mut().process_new_packets().unwrap();

        tls_conn.borrow_mut().reader().read(plaintext)
    }
}

pub async fn client_handshake(
    stream: tcp::Stream, config: Arc<rustls::ClientConfig>,
    server_name: rustls_pki_types::ServerName<'static>,
) -> Result<Client, std::io::Error> {
    let tls_client = Client {
        tcp_stream: stream,
        stream_impl: Rc::new(StreamImpl {
            tls_conn: RefCell::new(rustls::ClientConnection::new(config, server_name).unwrap()),
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
            for mut b in &bufs {
                tls_conn.borrow_mut().read_tls(&mut b)?;
            }
            tls_conn.borrow_mut().process_new_packets().unwrap();
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

impl Client {
    pub async fn send(&self, plaintext: &[u8]) -> Result<usize, std::io::Error> {
        self.stream_impl.send_pending.set(true);

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

    pub async fn recv(&self, plaintext: &mut [u8]) -> Result<usize, std::io::Error> {
        self.stream_impl.recv_pending.set(true);
        let tls_conn = &self.stream_impl.tls_conn;
        if !tls_conn.borrow().wants_read() {
            return tls_conn.borrow_mut().reader().read(plaintext);
        }

        let bufs = self.tcp_stream.recv().await?;

        for mut b in &bufs {
            tls_conn.borrow_mut().read_tls(&mut b)?;
        }
        tls_conn.borrow_mut().process_new_packets().unwrap();

        tls_conn.borrow_mut().reader().read(plaintext)
    }
}
