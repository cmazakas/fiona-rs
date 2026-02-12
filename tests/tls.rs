// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::sync::Arc;

use rustls_pki_types::pem::PemObject;

// fn to_hex(x: &[u8]) -> String {
//     x.iter().map(|b| format!("{:02x}", b)).collect()
// }

#[test]
fn tls_hello_world() {
    let cert =
        rustls_pki_types::CertificateDer::from_pem_file("tests/test_certs/server.crt").unwrap();

    let cert_key =
        rustls_pki_types::PrivateKeyDer::from_pem_file("tests/test_certs/server.key").unwrap();

    let server_tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], cert_key)
        .unwrap();

    let mut server_session = rustls::ServerConnection::new(Arc::new(server_tls_config)).unwrap();
    assert!(server_session.is_handshaking());
    assert!(server_session.wants_read());
    assert!(!server_session.wants_write());

    let mut cert_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };

    let root_ca =
        rustls_pki_types::CertificateDer::from_pem_file("tests/test_certs/ca.crt").unwrap();

    cert_store.add(root_ca).unwrap();

    let client_tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(cert_store))
        .with_no_client_auth();

    let mut client_session = rustls::ClientConnection::new(
        Arc::new(client_tls_config),
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

    client_session.read_tls(&mut &buf[..]).unwrap();
    client_session.process_new_packets().unwrap();

    assert!(!client_session.is_handshaking());
    assert!(client_session.wants_write());
    assert!(client_session.wants_read());

    buf.clear();
    while client_session.wants_write() {
        client_session.write_tls(&mut buf).unwrap();
    }

    server_session.read_tls(&mut &buf[..]).unwrap();
    server_session.process_new_packets().unwrap();

    assert!(!server_session.is_handshaking());
}
