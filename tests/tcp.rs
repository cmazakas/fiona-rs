use core::str;
use std::{future::Future, net::Ipv4Addr, task::Poll, time::Duration};

use futures::FutureExt;
use nix::errno::Errno;

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
fn test_tcp_multiple_accepts() {
    // Test that we can accept and hold onto multiple TCP sockets at a time.
    // Also test that we can properly recycle fds as well with the runtime.

    const NUM_CLIENTS: u64 = 500;

    static mut NUM_RUNS: u64 = 0;

    let params = &fiona::IoContextParams {
        sq_entries: 256,
        cq_entries: 1024,
        // We use 3 * NUM_CLIENTS because releasing the fds back to the runtime is deferred by
        // cancelling the timeout op associated with each socket object. Our test should
        // still properly test recycling of the fds because we have 2 * NUM_CLIENTS sockets per
        // iteration, and we iterate twice meaning we have 2 * 2 * NUM_CLIENTS total sockets being
        // used by the entire test over its lifetime.
        nr_files: (1 + 3 * NUM_CLIENTS).try_into().unwrap(),
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

#[test]
fn test_downcast_destruction() {
    // Want to prove that we can destruct the Client object via the Stream one.

    let ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let _stream;
    {
        let client = fiona::tcp::Client::new(ex);
        _stream = client.as_stream();
    }
}

#[test]
fn test_accept_select_ready_always() {
    // Test what happens when an accept would complete immediately succesfully
    // but the future isn't explicitly handled. This test is useful for handling the
    // oddities of Future manipulation that crates like futures facilitate. We went
    // to fundamentally test that we don't introduce fd leaks here and starve the
    // runtime of descriptors.

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.clone().spawn(async move {
        let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        assert!(acceptor.port() != 0);

        let port = acceptor.port();

        // Test that our accept() op completes immediately, but the future is
        // eager-dropped before we even see the CQE.
        let client = fiona::tcp::Client::new(ex.clone());
        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let x = futures::select! {
            _stream = acceptor.accept().fuse() => -1,
            x = futures::future::ready(1234).fuse() => x,
        };

        assert_eq!(x, 1234);

        let timer = fiona::time::Timer::new(ex.clone());
        timer.wait(Duration::from_millis(100)).await.unwrap();

        // Test that we can accept a new connection and that it works correctly.
        let client2 = fiona::tcp::Client::new(ex.clone());
        client2
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let stream = acceptor.accept().await.unwrap();
        let buf = stream
            .send(String::from("rawr!").into_bytes())
            .await
            .unwrap();

        assert!(buf.is_empty());

        // Test the case where an accept() op completes, we see the CQE and the Future +
        // task are still in scope so we still schedule resumption, but we never
        // actually poll() the AcceptFuture to completion.
        let client3 = fiona::tcp::Client::new(ex.clone());
        client3
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let mut accept_future = acceptor.accept();
        assert!(futures::poll!(&mut accept_future).is_pending());

        // must time-slice here
        timer.wait(Duration::from_millis(100)).await.unwrap();

        drop(accept_future);
        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn test_tcp_connect_timeout() {
    // Want to prove that our timeout works for connect() calls.
    // use one of the IP addresses from the test networks:
    // 192.0.2.0/24
    // https://en.wikipedia.org/wiki/Internet_Protocol_version_4#Special-use_addresses

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.clone().spawn(async move {
        let client = fiona::tcp::Client::new(ex);
        let m_ok = client.connect_ipv4(Ipv4Addr::new(192, 0, 2, 0), 3301).await;
        assert!(m_ok.is_err());
        assert_eq!(m_ok.unwrap_err(), Errno::ECANCELED);

        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn test_tcp_send_recv_hello_world() {
    // Want to test a simple send-recv pair, using the multishot recv op in the
    // background.

    static mut NUM_RUNS: u64 = 0;

    let message: &'static str = "hello, world! hopefuly this makes the message long enough such that we finally bundle lmao";

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    let bgid = 0;
    let num_bufs = 128;
    let buf_len = 8;
    ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

    let expected_bufs = message.len() / buf_len + if message.len() % buf_len > 0 { 1 } else { 0 };

    ex.clone().spawn(async move {
        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let bufs = stream.recv().await.unwrap();

        assert_eq!(bufs.len(), expected_bufs);

        let mut s = String::new();
        for buf in bufs.iter() {
            s.push_str(str::from_utf8(buf.as_slice()).unwrap());
        }

        assert_eq!(s, message);

        let msg = String::from(message).into_bytes();
        let _msg = stream.send(msg).await.unwrap();

        unsafe { NUM_RUNS += 1 };
    });

    ex.clone().spawn(async move {
        let client = fiona::tcp::Client::new(ex);

        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let msg = String::from(message).into_bytes();
        let _msg = client.send(msg).await.unwrap();

        client.set_buf_group(bgid);

        let bufs = client.recv().await.unwrap();

        assert_eq!(bufs.len(), expected_bufs);

        let mut s = String::new();
        for buf in bufs.iter() {
            s.push_str(str::from_utf8(buf.as_slice()).unwrap());
        }

        assert_eq!(s, message);

        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn test_tcp_recv_buffer_replenishing() {
    // Want to test that our recv op will eventually replensish the buffer set
    // upon resumption of a Future.

    static mut NUM_RUNS: u64 = 0;

    let message: &'static str = "hello, world! hopefuly this makes the message long enough such that we finally bundle lmao";

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    let bgid = 0;
    let num_bufs = 128;
    let buf_len = 8;
    let total_bytes = usize::try_from(num_bufs).unwrap() * buf_len;

    ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

    let expected_bufs = message.len() / buf_len + if message.len() % buf_len > 0 { 1 } else { 0 };

    ex.clone().spawn(async move {
        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let mut sent = 0;
        while sent < total_bytes {
            let bufs = stream.recv().await.unwrap();

            assert_eq!(bufs.len(), expected_bufs);

            let mut s = String::new();
            for buf in bufs.iter() {
                s.push_str(str::from_utf8(buf.as_slice()).unwrap());
            }

            assert_eq!(s, message);

            let msg = String::from(message).into_bytes();
            let msg = stream.send(msg).await.unwrap();
            sent += msg.len();
        }

        unsafe { NUM_RUNS += 1 };
    });

    ex.clone().spawn(async move {
        let client = fiona::tcp::Client::new(ex);

        client
            .connect_ipv4(Ipv4Addr::LOCALHOST, port)
            .await
            .unwrap();

        let mut received = 0;
        while received < total_bytes {
            let msg = String::from(message).into_bytes();
            let _msg = client.send(msg).await.unwrap();

            client.set_buf_group(bgid);

            let bufs = client.recv().await.unwrap();

            assert_eq!(bufs.len(), expected_bufs);

            let mut s = String::new();
            for buf in bufs.iter() {
                s.push_str(str::from_utf8(buf.as_slice()).unwrap());
            }

            assert_eq!(s, message);
            received += s.len();
        }

        unsafe { NUM_RUNS += 1 };
    });

    let n = ioc.run();
    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);
}
