// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::sync::Arc;

use rustls_pki_types::pem::PemObject;

#[test]
fn tls_hello_world() {
    let cert =
        rustls_pki_types::CertificateDer::from_pem_file("tests/test_certs/server.crt").unwrap();

    let cert_key =
        rustls_pki_types::PrivateKeyDer::from_pem_file("tests/test_certs/server.key").unwrap();

    let _server_tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], cert_key)
        .unwrap();

    let mut cert_store = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };

    let root_ca =
        rustls_pki_types::CertificateDer::from_pem_file("tests/test_certs/ca.crt").unwrap();

    cert_store.add(root_ca).unwrap();

    let _client_tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(Arc::new(cert_store))
        .with_no_client_auth();
}
