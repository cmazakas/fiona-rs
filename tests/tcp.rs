use std::net::Ipv4Addr;

#[test]
fn tcp_acceptor_hello_world() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex, Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    let _n = ioc.run();
    // assert_eq!(n, 1);
}
