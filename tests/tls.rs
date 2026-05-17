// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(clippy::redundant_closure_call)]

use std::{
    cell::RefCell,
    io::{Read, Write},
    net::Ipv6Addr,
    rc::Rc,
    sync::Arc,
    time::Duration,
};

use rand::{Rng, SeedableRng};
use rustls_pki_types::pem::PemObject;

// fn to_hex(x: &[u8]) -> String {
//     x.iter().map(|b| format!("{:02x}", b)).collect()
// }

const SERVER_CA: &str = "tests/test_certs/ca.crt";
const SERVER_CERT: &str = "tests/test_certs/server.crt";
const SERVER_PRIVATE_KEY: &str = "tests/test_certs/server.key";

fn make_server_config() -> Arc<rustls::ServerConfig> {
    let cert = rustls_pki_types::CertificateDer::from_pem_file(SERVER_CERT).unwrap();
    let cert_key = rustls_pki_types::PrivateKeyDer::from_pem_file(SERVER_PRIVATE_KEY).unwrap();

    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert], cert_key)
            .unwrap(),
    )
}

fn make_client_config() -> Arc<rustls::ClientConfig> {
    let mut cert_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };

    let root_ca = rustls_pki_types::CertificateDer::from_pem_file(SERVER_CA).unwrap();

    cert_store.add(root_ca).unwrap();

    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(Arc::new(cert_store))
            .with_no_client_auth(),
    )
}

fn make_tls_socket_pair(
    ioc: &mut fiona::IoContext, bgid: u16, num_bufs: u32, buf_len: usize,
) -> (fiona::tls::TlsStream, fiona::tls::TlsClient) {
    let ex = ioc.get_executor();
    let acceptor = fiona::net::TcpListener::bind_ipv6(&ex, Ipv6Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    if !ex.has_buf_group(bgid) {
        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();
    }

    let server_conn = Rc::new(RefCell::new(None));
    let client_conn = Rc::new(RefCell::new(None));

    let server_conn_copy = server_conn.clone();
    ex.spawn(async move {
        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let tls_stream = fiona::tls::server_handshake(stream, make_server_config())
            .await
            .unwrap();

        *server_conn_copy.borrow_mut() = Some(tls_stream);
    });

    let client_conn_copy = client_conn.clone();
    ex.clone().spawn(async move {
        let stream = fiona::net::TcpClient::new(&ex)
            .connect_ipv6(Ipv6Addr::LOCALHOST, port)
            .await
            .unwrap();

        stream.set_buf_group(bgid);

        let tls_stream = fiona::tls::client_handshake(
            stream,
            make_client_config(),
            "localhost".try_into().unwrap(),
        )
        .await
        .unwrap();

        *client_conn_copy.borrow_mut() = Some(tls_stream);
    });

    ioc.run();

    (server_conn.borrow_mut().take().unwrap(), client_conn.borrow_mut().take().unwrap())
}

#[test]
fn tls_hello_world() {
    let mut server_session = rustls::ServerConnection::new(make_server_config()).unwrap();
    assert!(server_session.is_handshaking());
    assert!(server_session.wants_read());
    assert!(!server_session.wants_write());

    let mut client_session = rustls::ClientConnection::new(
        make_client_config(),
        rustls_pki_types::ServerName::try_from("localhost").unwrap(),
    )
    .unwrap();

    assert!(client_session.is_handshaking());
    assert!(!client_session.wants_read());
    assert!(client_session.wants_write());

    let mut buf = Vec::<u8>::new();
    while client_session.wants_write() {
        client_session.write_tls(&mut buf).unwrap();
    }

    while server_session.wants_read() {
        server_session.read_tls(&mut &buf[..]).unwrap();
        server_session.process_new_packets().unwrap();
    }
    buf.clear();

    while server_session.wants_write() {
        server_session.write_tls(&mut buf).unwrap();
    }

    while client_session.is_handshaking() {
        let n = client_session.read_tls(&mut &buf[..]).unwrap();
        client_session.process_new_packets().unwrap();
        buf.drain(0..n);
        assert!(client_session.wants_read());
    }
    buf.clear();

    while client_session.wants_write() {
        client_session.write_tls(&mut buf).unwrap();
    }

    while server_session.is_handshaking() {
        let n = server_session.read_tls(&mut &buf[..]).unwrap();
        server_session.process_new_packets().unwrap();
        buf.drain(0..n);
    }
    buf.clear();

    assert_eq!(client_session.protocol_version().unwrap(), rustls::ProtocolVersion::TLSv1_3);
    assert_eq!(server_session.protocol_version().unwrap(), rustls::ProtocolVersion::TLSv1_3);

    // Post-handshake and we still have TLS data to write.
    assert!(server_session.wants_write());

    // Leave this code commented out to test a unified write of pending handshake
    // data along with encrypted application data, set down below.

    // while server_session.wants_write() {
    //     assert!(server_session.write_tls(&mut buf).unwrap() > 0);
    // }
    // assert!(!server_session.wants_write());
    // client_session.read_tls(&mut &buf[..]).unwrap();
    // client_session.process_new_packets().unwrap();

    assert!(client_session.wants_read());
    assert!(!client_session.wants_write());
    assert!(server_session.wants_read());

    let client_msg = "I bestow the heads of virgins and the first-born sons!".as_bytes();
    let server_msg = "...within these monuments of stone!".as_bytes();

    client_session.writer().write_all(client_msg).unwrap();
    server_session.writer().write_all(server_msg).unwrap();

    buf.clear();
    while server_session.wants_write() {
        server_session.write_tls(&mut buf).unwrap();
    }

    client_session.read_tls(&mut &buf[..]).unwrap();
    client_session.process_new_packets().unwrap();

    buf.clear();
    while client_session.wants_write() {
        client_session.write_tls(&mut buf).unwrap();
    }

    server_session.read_tls(&mut &buf[..]).unwrap();
    server_session.process_new_packets().unwrap();

    buf.clear();
    if let Err(err) = server_session.reader().read_to_end(&mut buf) {
        assert!(err.kind() == std::io::ErrorKind::WouldBlock);
    } else {
        unreachable!()
    };
    assert_eq!(buf, client_msg);

    buf.clear();
    if let Err(err) = client_session.reader().read_to_end(&mut buf) {
        assert!(err.kind() == std::io::ErrorKind::WouldBlock);
    } else {
        unreachable!()
    };
    assert_eq!(buf, server_msg);

    buf.clear();
    client_session.send_close_notify();
    assert!(client_session.wants_write());
    while client_session.wants_write() {
        client_session.write_tls(&mut buf).unwrap();
    }

    server_session.read_tls(&mut &buf[..]).unwrap();
    let state = server_session.process_new_packets().unwrap();
    assert!(state.peer_has_closed());

    buf.clear();
    server_session.send_close_notify();
    assert!(server_session.wants_write());
    while server_session.wants_write() {
        server_session.write_tls(&mut buf).unwrap();
    }

    client_session.read_tls(&mut &buf[..]).unwrap();
    client_session.process_new_packets().unwrap();

    assert!(!client_session.wants_read());
    assert!(!client_session.wants_write());

    assert!(!server_session.wants_read());
    assert!(!server_session.wants_write());
}

#[test]
fn tls_handshake() {
    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();
    let acceptor = fiona::net::TcpListener::bind_ipv6(&ex, Ipv6Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.register_buf_group(1234, 1024, 256).unwrap();
    ex.register_buf_group(4321, 1024, 256).unwrap();

    ex.spawn((async |acceptor: fiona::net::TcpListener| {
        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(1234);

        let _tls_stream = fiona::tls::server_handshake(stream, make_server_config())
            .await
            .unwrap();

        let stream2 = acceptor.accept().await.unwrap();
        stream2.set_buf_group(1234);

        let Err(tls_error) = fiona::tls::server_handshake(stream2, make_server_config()).await
        else {
            unreachable!()
        };

        let fiona::tls::Error::Tls(rustls::Error::HandshakeNotComplete) = tls_error else {
            eprintln!("{tls_error:?}");
            unreachable!()
        };
    })(acceptor));

    ex.spawn((async |ex: fiona::Executor, port: u16| {
        let client = fiona::net::TcpClient::new(&ex)
            .connect_ipv6(Ipv6Addr::LOCALHOST, port)
            .await
            .unwrap();

        client.set_buf_group(4321);

        let _tls_client = fiona::tls::client_handshake(
            client,
            make_client_config(),
            "localhost".try_into().unwrap(),
        )
        .await
        .unwrap();

        let client = fiona::net::TcpClient::new(&ex)
            .connect_ipv6(Ipv6Addr::LOCALHOST, port)
            .await
            .unwrap();

        client.set_buf_group(4321);

        let Err(tls_error) = fiona::tls::client_handshake(
            client,
            make_client_config(),
            "www.google.com".try_into().unwrap(),
        )
        .await
        else {
            unreachable!()
        };

        let fiona::tls::Error::Tls(rustls::Error::InvalidCertificate(_)) = tls_error else {
            eprintln!("{:?}", tls_error);
            unreachable!()
        };
    })(ex.clone(), port));

    assert_eq!(ioc.run(), 2);
}

#[test]
fn tls_send_recv() {
    // Test a simple exchange of fixed-sized messages.

    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();
    let acceptor = fiona::net::TcpListener::bind_ipv6(&ex, Ipv6Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.register_buf_group(1234, 1024, 256).unwrap();
    ex.register_buf_group(4321, 1024, 256).unwrap();

    ex.spawn((async |acceptor: fiona::net::TcpListener| {
        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(1234);

        let tls_stream = fiona::tls::server_handshake(stream, make_server_config())
            .await
            .unwrap();

        let text = "Hello, world! This is plaintext from the server!";

        let n = tls_stream.write(text.as_bytes()).unwrap();
        assert_eq!(n, text.len());

        let n = tls_stream.flush(1024).await.unwrap();
        assert!(n > text.len());

        let mut msg = Vec::new();
        let n = tls_stream.read(&mut msg).await.unwrap();

        assert_eq!(msg.len(), n);
        assert_eq!(
            str::from_utf8(&msg[..]).unwrap(),
            "Hello, world! This is plaintext from the client!"
        );
    })(acceptor));

    ex.spawn((async |ex: fiona::Executor, port: u16| {
        let client = fiona::net::TcpClient::new(&ex)
            .connect_ipv6(Ipv6Addr::LOCALHOST, port)
            .await
            .unwrap();

        client.set_buf_group(4321);

        let tls_client = fiona::tls::client_handshake(
            client,
            make_client_config(),
            "localhost".try_into().unwrap(),
        )
        .await
        .unwrap();

        let text = "Hello, world! This is plaintext from the client!";

        let n = tls_client.write(text.as_bytes()).unwrap();
        assert_eq!(n, text.len());

        let n = tls_client.flush(1024).await.unwrap();
        assert!(n > text.len());

        let mut buf = Vec::new();
        let n = tls_client.read(&mut buf).await.unwrap();

        assert_eq!(
            str::from_utf8(&buf[..n]).unwrap(),
            "Hello, world! This is plaintext from the server!"
        );
    })(ex.clone(), port));

    assert_eq!(ioc.run(), 2);
}

#[test]
fn tls_large_send() {
    // Test that our code doesn't drop any octets when sending something is
    // sufficiently large that it needs multiple TCP sends.

    const MAX_SEND_SIZE: usize = 16 * 1024;

    let msg_len = 128 * 1024;
    let mut message = vec![0; msg_len];
    {
        let mut rng = rand::rngs::StdRng::from_os_rng();
        rand::RngCore::fill_bytes(&mut rng, &mut message);
    }

    let message = Rc::new(message);

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 1024);

    let server_msg = message.clone();
    ex.spawn(async move {
        let mut buf = server_msg.as_slice();
        let mut total_written = 0;
        loop {
            if total_written < server_msg.len() {
                let n = tls_stream.write(buf).unwrap();
                assert!(n > 0);
                assert!(n < server_msg.len());
                total_written += n;
                buf = &buf[n..];
                if !buf.is_empty() {
                    let n = tls_stream.write(buf).unwrap();
                    assert_eq!(n, 0);
                }
            }

            let sent = tls_stream.flush(MAX_SEND_SIZE).await.unwrap();
            if total_written == server_msg.len() && sent == 0 {
                break;
            }
        }

        let mut msg = Vec::with_capacity(msg_len);
        let mut n = 0;

        while n < server_msg.len() {
            n += tls_stream.read(&mut msg).await.unwrap();
        }

        assert!(msg == *server_msg);
    });

    let client_msg = message.clone();
    ex.clone().spawn(async move {
        let mut buf = client_msg.as_slice();
        let mut total_written = 0;
        loop {
            if total_written < client_msg.len() {
                let n = tls_client.write(buf).unwrap();
                assert!(n > 0);
                assert!(n < client_msg.len());
                total_written += n;
                buf = &buf[n..];
                if !buf.is_empty() {
                    let n = tls_client.write(buf).unwrap();
                    assert_eq!(n, 0);
                }
            }

            let sent = tls_client.flush(MAX_SEND_SIZE).await.unwrap();
            if total_written == client_msg.len() && sent == 0 {
                break;
            }
        }

        let mut msg = Vec::new();
        let mut n = 0;

        while n < client_msg.len() {
            n += tls_client.read(&mut msg).await.unwrap();
        }

        assert!(msg == *client_msg);
    });

    assert_eq!(ioc.run(), 2);
}

#[test]
fn tls_large_send_randomized() {
    // Test that we can handle large sends, under randomized conditions.

    const MAX_SEND_SIZE: usize = 16 * 1024;
    const MAX_WRITE: usize = 64 * 1024;

    let msg_len = 1024 * 1024;

    let client_message = {
        let mut message = vec![0; msg_len];
        let mut rng = rand::rngs::StdRng::from_os_rng();
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    let server_message = {
        let mut message = vec![0; msg_len];
        let mut rng = rand::rngs::StdRng::from_os_rng();
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 16 * 1024, 1024);

    let server_msg1 = server_message.clone();
    let client_msg1 = client_message.clone();
    ex.spawn(async move {
        let mut rng = rand::rng();

        let mut buf = server_msg1.as_slice();
        let mut total_written = 0;
        loop {
            if total_written < server_msg1.len() {
                loop {
                    let n = tls_stream
                        .write(&buf[..rng.random_range(1..=MAX_WRITE).min(buf.len())])
                        .unwrap();

                    if n == 0 {
                        break;
                    }

                    assert!(n > 0);
                    assert!(n < server_msg1.len());
                    total_written += n;
                    buf = &buf[n..];
                }
            }

            let sent = tls_stream
                .flush(rng.random_range(1..=MAX_SEND_SIZE))
                .await
                .unwrap();

            if total_written == server_msg1.len() && sent == 0 {
                break;
            }
        }

        let mut msg = Vec::with_capacity(msg_len);
        let mut n = 0;

        while n < client_msg1.len() {
            n += tls_stream.read(&mut msg).await.unwrap();
        }

        assert!(msg == *client_msg1);
    });

    let server_msg2 = server_message.clone();
    let client_msg2 = client_message.clone();
    ex.clone().spawn(async move {
        let mut rng = rand::rng();

        let mut buf = client_msg2.as_slice();
        let mut total_written = 0;
        loop {
            if total_written < client_msg2.len() {
                loop {
                    let n = tls_client
                        .write(&buf[..rng.random_range(1..=MAX_WRITE).min(buf.len())])
                        .unwrap();

                    if n == 0 {
                        break;
                    }

                    assert!(n > 0);
                    assert!(n < client_msg2.len());
                    total_written += n;
                    buf = &buf[n..];
                }
            }

            let sent = tls_client
                .flush(rng.random_range(1..=MAX_SEND_SIZE))
                .await
                .unwrap();

            if total_written == client_msg2.len() && sent == 0 {
                break;
            }
        }

        let mut msg = Vec::new();
        let mut n = 0;

        while n < server_msg2.len() {
            n += tls_client.read(&mut msg).await.unwrap();
        }

        assert!(msg == *server_msg2);
    });

    assert_eq!(ioc.run(), 2);
}

#[test]
fn tls_shutdown() {
    // Test what happens to our TLS streams when the peer sends a close_notify
    // in the middle of a normal recv operation.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 1024);

    ex.spawn(async move {
        let mut buf = Vec::new();

        let read = tls_stream.read(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert_eq!(&buf[..read], b"Hello, world!");
        buf.clear();

        let read = tls_stream.read(&mut buf).await.unwrap();
        assert!(buf.is_empty());
        assert_eq!(read, 0);

        let sent = tls_stream.flush(8 * 1024).await.unwrap();
        assert!(sent > 0);

        let written = tls_stream.write(b"rawr").unwrap();
        assert_eq!(written, 0);

        let sent = tls_stream.flush(4 * 1024).await.unwrap();
        assert_eq!(sent, 0);
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            tls_client.write(b"Hello, world!").unwrap();
            tls_client.flush(1024).await.unwrap();

            tls_client.write_shutdown();
            tls_client.flush(32 * 1024).await.unwrap();

            let written = tls_client.write(b"rawr").unwrap();
            assert_eq!(written, 0);

            let sent = tls_client.flush(4 * 1024).await.unwrap();
            assert_eq!(sent, 0);

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 2);

    // Should be the dual of what's above.

    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 1024);

    ex.spawn(async move {
        let mut buf = Vec::new();

        let read = tls_client.read(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert_eq!(&buf[..read], b"Hello, world!");
        buf.clear();

        let read = tls_client.read(&mut buf).await.unwrap();
        assert!(buf.is_empty());
        assert_eq!(read, 0);

        let sent = tls_client.flush(8 * 1024).await.unwrap();
        assert!(sent > 0);

        let written = tls_client.write(b"rawr").unwrap();
        assert_eq!(written, 0);

        let sent = tls_client.flush(4 * 1024).await.unwrap();
        assert_eq!(sent, 0);
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            tls_stream.write(b"Hello, world!").unwrap();
            tls_stream.flush(1024).await.unwrap();

            tls_stream.write_shutdown();
            tls_stream.flush(32 * 1024).await.unwrap();

            let written = tls_stream.write(b"rawr").unwrap();
            assert_eq!(written, 0);

            let sent = tls_stream.flush(4 * 1024).await.unwrap();
            assert_eq!(sent, 0);

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 2);
}

#[test]
fn tls_concurrent_read_write() {
    // Test that a concurrent read() and write() + flush() call are sound together.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 1024);

    ex.spawn(async move {
        let mut buf = Vec::new();

        let read = tls_stream.read(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert_eq!(&buf[..read], b"Hello, world!");
        buf.clear();

        let read = tls_stream.read(&mut buf).await.unwrap();
        assert!(!buf.is_empty());
        assert_eq!(&buf[..read], b"Hello world! Again, this time!");
        buf.clear();
    });

    ex.spawn({
        let ex = ex.clone();
        async move {
            tls_client.write(b"Hello, world!").unwrap();

            ex.spawn({
                let tls_client = tls_client.clone();
                async move {
                    tls_client.write(b"Hello world! Again, this time!").unwrap();
                }
            });

            tls_client.flush(1024).await.unwrap();
            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
            tls_client.flush(1024).await.unwrap();
        }
    });

    ioc.run();
}

#[test]
fn tls_concurrent_read_write_large() {
    // Test that a concurrent read() and write() + flush() call are sound together,
    // but for a large message.

    const MAX_SEND: usize = 4 * 1024;

    let msg_len = 1024 * 1024;

    let mut rng = rand::rngs::StdRng::from_os_rng();

    let client_message = {
        let mut message = vec![0; msg_len];
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    let server_message = {
        let mut message = vec![0; msg_len];
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 16 * 1024);

    ex.spawn({
        let (tls_stream, client_message) = (tls_stream.clone(), client_message.clone());
        async move {
            let mut n = 0;
            let mut buf = Vec::new();

            while buf.len() < client_message.len() {
                n += tls_stream.read(&mut buf).await.unwrap();
            }

            assert_eq!(n, client_message.len());
            assert_eq!(buf, &client_message[..]);
        }
    });

    ex.spawn({
        let (tls_stream, server_message) = (tls_stream.clone(), server_message.clone());
        async move {
            let mut num_written = 0;
            loop {
                if num_written < server_message.len() {
                    let n = tls_stream.write(&server_message[num_written..]).unwrap();
                    num_written += n;
                }

                let n = tls_stream.flush(MAX_SEND).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        }
    });

    ex.spawn({
        let (tls_client, server_message) = (tls_client.clone(), server_message.clone());
        async move {
            let mut n = 0;
            let mut buf = Vec::new();

            while buf.len() < server_message.len() {
                n += tls_client.read(&mut buf).await.unwrap();
            }

            assert_eq!(n, server_message.len());
            assert_eq!(buf, &server_message[..]);
        }
    });

    ex.spawn({
        let (tls_client, client_message) = (tls_client.clone(), client_message.clone());
        async move {
            let mut num_written = 0;
            loop {
                if num_written < client_message.len() {
                    let n = tls_client.write(&client_message[num_written..]).unwrap();
                    num_written += n;
                }

                let n = tls_client.flush(MAX_SEND).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 4);
}

#[test]
fn tls_concurrent_read_write_intermittent_shutdown() {
    // Test TLS shutdown semantics during a concurrent read(), write() loop.

    const MAX_SEND: usize = 64 * 1024;

    let msg_len = 1024 * 1024;

    let mut rng = rand::rngs::StdRng::from_os_rng();

    let client_message = {
        let mut message = vec![0; msg_len];
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    let server_message = {
        let mut message = vec![0; msg_len];
        rand::RngCore::fill_bytes(&mut rng, &mut message);
        Rc::new(message)
    };

    assert_eq!(client_message.len(), msg_len);
    assert_eq!(server_message.len(), msg_len);

    let write_close_after = rng.random_range(1..msg_len / 2);

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    let (tls_stream, tls_client) = make_tls_socket_pair(&mut ioc, 1234, 1024, 16 * 1024);

    ex.spawn({
        let (tls_stream, client_message) = (tls_stream.clone(), client_message.clone());
        async move {
            let mut buf = Vec::new();

            while buf.len() < client_message.len() {
                let n = tls_stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }

            assert!(buf.len() < client_message.len());
        }
    });

    ex.spawn({
        let (tls_stream, server_message) = (tls_stream.clone(), server_message.clone());
        async move {
            let mut num_written = 0;
            loop {
                if num_written < server_message.len() {
                    let n = tls_stream.write(&server_message[num_written..]).unwrap();
                    num_written += n;
                }

                let n = tls_stream.flush(MAX_SEND).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        }
    });

    ex.spawn({
        let (tls_client, server_message) = (tls_client.clone(), server_message.clone());
        async move {
            let mut buf = Vec::new();

            while buf.len() < server_message.len() {
                let n = tls_client.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
            }

            assert!(buf.len() < server_message.len());
        }
    });

    ex.spawn({
        let (tls_client, client_message) = (tls_client.clone(), client_message.clone());
        async move {
            let mut num_written = 0;
            loop {
                if num_written < client_message.len() {
                    let n = tls_client.write(&client_message[num_written..]).unwrap();
                    num_written += n;
                }

                if num_written >= write_close_after {
                    tls_stream.write_shutdown();
                }

                let n = tls_client.flush(MAX_SEND).await.unwrap();
                if n == 0 {
                    break;
                }
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 4);
}
