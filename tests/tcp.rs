use std::{future::Future, net::Ipv4Addr, task::Poll, time::Duration};

struct WakerFuture;

impl Future for WakerFuture {
    type Output = std::task::Waker;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(cx.waker().clone())
    }
}

#[test]
fn tcp_acceptor_eager_drop() {
    // Test basic handling of eager-dropping a TCP socket

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    ex.spawn(async move {
        for _ in 0..3 {
            let mut f = acceptor.accept();

            let w = WakerFuture.await;
            assert!(std::pin::pin!(&mut f)
                .poll(&mut std::task::Context::from_waker(&w))
                .is_pending());

            drop(f);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn tcp_acceptor_hello_world() {
    // Hello World example for TCP accept().

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    let port = acceptor.port();

    ex.spawn(async move {
        let _stream = acceptor.accept().await.unwrap();
        unsafe { NUM_RUNS += 1 };
    });

    let client = fiona::tcp::Client::new(ex.clone());
    ex.spawn(async move {
        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn tcp_multiple_accepts() {
    // Test that we can accept and hold onto multiple TCP sockets at a time.
    // Also test that we can properly recycle fds as well with the runtime.

    const NUM_CLIENTS: u64 = 500;

    static mut NUM_RUNS: u64 = 0;

    let params = &fiona::IoContextParams {
        sq_entries: 256,
        cq_entries: 1024,
        nr_files: (1 + 2 * NUM_CLIENTS).try_into().unwrap(),
    };

    let mut ioc = fiona::IoContext::with_params(params);
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    let port = acceptor.port();

    ex.clone().spawn(async move {
        for _ in 0..2 {
            for _ in 0..NUM_CLIENTS {
                let client = fiona::tcp::Client::new(ex.clone());
                ex.spawn(async move {
                    client
                        .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                        .await
                        .unwrap();

                    unsafe { NUM_RUNS += 1 };
                });
            }

            let mut streams = Vec::new();
            for _ in 0..NUM_CLIENTS {
                let stream = acceptor.accept().await.unwrap();
                streams.push(stream);
            }
            assert_eq!(streams.len(), NUM_CLIENTS.try_into().unwrap());
        }
        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 2 * NUM_CLIENTS + 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn tcp_send_hello_world() {
    // Test two peers send()'ing each other a message, no recv().

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    let port = acceptor.port();

    ex.spawn(async move {
        let stream = acceptor.accept().await.unwrap();

        let buf = String::from("hello, world!");
        let buf = stream.send(buf.into_bytes()).await.unwrap();
        assert!(buf.is_empty());

        unsafe { NUM_RUNS += 1 };
    });

    let client = fiona::tcp::Client::new(ex.clone());
    ex.clone().spawn(async move {
        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let buf = String::from("i love io_uring");
        let buf = client.send(buf.into_bytes()).await.unwrap();
        assert!(buf.is_empty());

        let timer = fiona::time::Timer::new(ex);
        timer.wait(Duration::from_millis(250)).await.unwrap();

        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
#[should_panic = "rawr"]
fn tcp_send_hello_world_throwing() {
    // Test that during panic we can clean up properly when the tasks touch the
    // runtime in their destructor (such as TCP sockets returning their fds,
    // scheduling close() calls, etc.).

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    let port = acceptor.port();

    ex.spawn(async move {
        let stream = acceptor.accept().await.unwrap();

        let buf = String::from("hello, world!");
        let buf = stream.send(buf.into_bytes()).await.unwrap();
        assert!(buf.is_empty());
        panic!("rawr");
    });

    let client = fiona::tcp::Client::new(ex.clone());
    ex.clone().spawn(async move {
        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let timer = fiona::time::Timer::new(ex);
        timer.wait(Duration::from_millis(250)).await.unwrap();

        unsafe { NUM_RUNS += 1 };
    });

    let _n = ioc.run();
}
