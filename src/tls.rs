// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(clippy::struct_field_names, clippy::await_holding_refcell_ref)]

use std::{cell::RefCell, rc::Rc, sync::Arc};

use crate::tcp;

pub struct Stream {
    tcp_stream: tcp::Stream,
    tls_conn: Rc<std::cell::RefCell<rustls::ServerConnection>>,
    buf: Vec<u8>,
}

pub struct Client {
    tcp_stream: tcp::Stream,
    tls_conn: Rc<std::cell::RefCell<rustls::ClientConnection>>,
    buf: Vec<u8>,
}

pub async fn server_handshake(
    stream: tcp::Stream, config: Arc<rustls::ServerConfig>,
) -> Result<Stream, std::io::Error> {
    let mut tls_stream = Stream {
        tcp_stream: stream,
        tls_conn: Rc::new(RefCell::new(rustls::ServerConnection::new(config).unwrap())),
        buf: Vec::new(),
    };

    let mut borrow_guard = tls_stream.tls_conn.borrow_mut();
    let tls_conn = &mut *borrow_guard;
    let mut buf = std::mem::take(&mut tls_stream.buf);

    while tls_conn.is_handshaking() {
        if tls_conn.wants_write() {
            tls_conn.write_tls(&mut buf).unwrap();

            let (n, send_buf) = tls_stream.tcp_stream.send(buf).await;
            assert!(n.is_ok());
            buf = send_buf;
            buf.clear();
        }

        if tls_conn.wants_read() && tls_conn.is_handshaking() {
            let bufs = tls_stream.tcp_stream.recv().await.unwrap();
            assert!(!bufs.is_empty());

            for mut buf in &bufs {
                tls_conn.read_tls(&mut buf).unwrap();
            }
            tls_conn.process_new_packets().unwrap();
        }
    }

    drop(borrow_guard);

    tls_stream.buf = buf;

    Ok(tls_stream)
}

pub async fn client_handshake(
    client: tcp::Client, config: Arc<rustls::ClientConfig>,
    server_name: rustls_pki_types::ServerName<'static>,
) -> Result<Client, std::io::Error> {
    let mut tls_client = Client {
        tcp_stream: client.as_stream(),
        tls_conn: Rc::new(RefCell::new(
            rustls::ClientConnection::new(config, server_name).unwrap(),
        )),
        buf: Vec::new(),
    };

    let mut borrow_guard = tls_client.tls_conn.borrow_mut();
    let tls_conn = &mut *borrow_guard;
    let mut buf = std::mem::take(&mut tls_client.buf);

    while tls_conn.is_handshaking() {
        if tls_conn.wants_write() {
            tls_conn.write_tls(&mut buf).unwrap();
            let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
            assert!(n.unwrap() > 0);
            buf = send_buf;
            buf.clear();
        }

        if tls_conn.wants_read() {
            let bufs = tls_client.tcp_stream.recv().await.unwrap();
            assert!(!bufs.is_empty());
            for mut buf in &bufs {
                tls_conn.read_tls(&mut buf).unwrap();
            }
            tls_conn.process_new_packets().unwrap();
        }
    }

    if tls_conn.wants_write() {
        tls_conn.write_tls(&mut buf).unwrap();
        let (n, send_buf) = tls_client.tcp_stream.send(buf).await;
        assert!(n.unwrap() > 0);
        buf = send_buf;
        buf.clear();
    }

    drop(borrow_guard);

    Ok(tls_client)
}
