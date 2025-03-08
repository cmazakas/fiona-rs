use core::str;
use std::{
    future::Future,
    net::Ipv4Addr,
    panic::{AssertUnwindSafe, catch_unwind},
    task::Poll,
    time::Duration,
};

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use nix::{errno::Errno, libc::EISCONN};
use rand::SeedableRng;

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
            assert!(
                std::pin::pin!(&mut f)
                    .poll(&mut std::task::Context::from_waker(&w))
                    .is_pending()
            );

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
fn tcp_connect_timeout() {
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
fn tcp_send_recv_hello_world() {
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
fn tcp_recv_buffer_replenishing() {
    // Want to test that our recv op will eventually replensish the buffer set
    // upon resumption of a Future.

    static mut NUM_RUNS: u64 = 0;

    let message_len = 1337;
    let mut rng = rand::rngs::StdRng::from_os_rng();
    let mut message = vec![0; message_len];
    rand::RngCore::fill_bytes(&mut rng, &mut message);

    let message = message;
    let chunk_size = 90;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    let bgid = 0;
    let num_bufs = 64;
    let buf_len = 8;
    let total_bytes = usize::try_from(num_bufs).unwrap() * buf_len;

    assert!(message.len() > total_bytes);

    ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

    {
        let message = message.clone();
        ex.clone().spawn(async move {
            let stream = acceptor.accept().await.unwrap();
            stream.set_buf_group(bgid);

            let mut acc_msg = Vec::new();

            while acc_msg.len() < message_len {
                let bufs = stream.recv().await.unwrap();

                let mut msg = Vec::new();
                for buf in bufs.iter() {
                    msg.extend_from_slice(buf);
                }

                acc_msg.extend_from_slice(&msg);

                let msg = stream.send(msg).await.unwrap();
                assert!(msg.is_empty());
            }

            assert_eq!(acc_msg, message);
            unsafe { NUM_RUNS += 1 };
        });
    }

    {
        let message = message.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex);

            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            client.set_buf_group(bgid);

            let mut acc_msg = Vec::new();

            for chunk in message.chunks(chunk_size) {
                let msg = chunk.to_vec();
                let msg = client.send(msg).await.unwrap();
                assert!(msg.is_empty());

                let bufs = client.recv().await.unwrap();

                for buf in bufs.iter() {
                    acc_msg.extend_from_slice(buf);
                }
            }

            assert_eq!(acc_msg, message);
            unsafe { NUM_RUNS += 1 };
        });
    }

    let n = ioc.run();
    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

fn bufs_to_string(bufs: Vec<Vec<u8>>) -> String {
    let mut s = String::new();
    for buf in bufs {
        s.push_str(str::from_utf8(&buf).unwrap());
    }
    s
}

fn flatten_bufs(bufs: Vec<Vec<u8>>) -> Vec<u8> {
    let mut v = Vec::new();
    for buf in bufs {
        v.extend_from_slice(buf.as_slice());
    }
    v
}

#[test]
fn tcp_recv_timeout() {
    // Test that our recv operation can timeout and then be restarted.

    async fn server_timeout(ex: fiona::Executor) {
        let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 0;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex.clone());

            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            client.set_buf_group(bgid);

            let msg = String::from("hello, world!");
            let mut msg = client.send(msg.into_bytes()).await.unwrap();
            assert!(msg.is_empty());
            msg.extend_from_slice("hello, world!".as_bytes());

            let timer = fiona::time::Timer::new(ex);
            timer.wait(Duration::from_millis(1000)).await.unwrap();

            let msg = client.send(msg).await.unwrap();
            assert!(msg.is_empty());

            unsafe { NUM_RUNS += 1 };
        });

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);
        stream.set_timeout(Duration::from_millis(500));

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        unsafe { NUM_RUNS += 1 };
    }

    async fn client_timeout(ex: fiona::Executor) {
        let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 1;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        let ex2 = ex.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex2.clone());

            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            client.set_buf_group(bgid);
            client.set_timeout(Duration::from_millis(500));

            let bufs = client.recv().await.unwrap();
            assert_eq!(bufs_to_string(bufs), "wonderful day");

            let err = client.recv().await.unwrap_err();
            assert_eq!(err, Errno::ECANCELED);

            let bufs = client.recv().await.unwrap();
            assert_eq!(bufs_to_string(bufs), "wonderful day");

            let err = client.recv().await.unwrap_err();
            assert_eq!(err, Errno::ECANCELED);

            unsafe { NUM_RUNS += 1 };
        });

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let msg = String::from("wonderful day");
        let mut msg = stream.send(msg.into_bytes()).await.unwrap();
        assert!(msg.is_empty());
        msg.extend_from_slice("wonderful day".as_bytes());

        let timer = fiona::time::Timer::new(ex);
        timer.wait(Duration::from_millis(1000)).await.unwrap();

        let msg = stream.send(msg).await.unwrap();
        assert!(msg.is_empty());

        unsafe { NUM_RUNS += 1 };
    }

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.clone().spawn(server_timeout(ex.clone()));
    ex.clone().spawn(client_timeout(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 4);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn tcp_connection_stress_test_no_cq_overflow() {
    const NR_FILES: u32 = 500;
    const CQ_ENTRIES: u32 = 16 * 1024;

    const SERVER_BGID: u16 = 27;
    const CLIENT_BGID: u16 = 72;
    const MSG_LEN: usize = 1024;

    const TOTAL_CONNS: u32 = 3 * NR_FILES;

    fn make_io_context() -> fiona::IoContext {
        let params = &fiona::IoContextParams {
            sq_entries: 256,
            cq_entries: CQ_ENTRIES,
            nr_files: 2 * NR_FILES,
        };

        fiona::IoContext::with_params(params)
    }

    fn client(port: u16) {
        let mut ioc = make_io_context();
        let ex = ioc.get_executor();
        ex.register_buf_group(CLIENT_BGID, 4 * 1024, 1024).unwrap();

        ex.clone().spawn(async move {
            for _ in 0..(TOTAL_CONNS / NR_FILES) {
                let mut tasks: FuturesUnordered<fiona::SpawnFuture<u32>> = (0..NR_FILES)
                    .map(|idx| {
                        let ex2 = ex.clone();
                        ex.clone().spawn(async move {
                            let mut message = vec![0; MSG_LEN];
                            {
                                let mut rng = rand::rngs::StdRng::from_os_rng();
                                rand::RngCore::fill_bytes(&mut rng, &mut message);
                            }

                            let timer = fiona::time::Timer::new(ex2.clone());
                            timer
                                .wait(Duration::from_millis(message[0].into()))
                                .await
                                .unwrap();

                            let client = fiona::tcp::Client::new(ex2);
                            client
                                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                                .await
                                .unwrap();

                            let msg_copy = message.clone();
                            let message = client.send(message).await.unwrap();
                            assert!(message.is_empty());

                            client.set_buf_group(CLIENT_BGID);

                            let bufs = client.recv().await.unwrap();
                            let buf = flatten_bufs(bufs);

                            assert_eq!(buf, msg_copy);
                            idx
                        })
                    })
                    .collect();

                while !tasks.is_empty() {
                    tasks.next().await.unwrap();
                }
            }
        });

        let n = ioc.run();
        assert_eq!(n, (1 + TOTAL_CONNS).into());
    }

    let mut ioc = make_io_context();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.clone().spawn(async move {
        ex.register_buf_group(SERVER_BGID, 4 * 1024, 1024).unwrap();

        let mut conns = 0;
        while conns < TOTAL_CONNS {
            let stream = acceptor.accept().await.unwrap();
            ex.clone().spawn(async move {
                stream.set_buf_group(SERVER_BGID);

                let bufs = stream.recv().await.unwrap();
                let send_buf = flatten_bufs(bufs);
                assert!(!send_buf.is_empty());

                let send_buf = stream.send(send_buf).await.unwrap();

                assert!(send_buf.is_empty());
            });
            conns += 1;
        }
    });

    let t = std::thread::spawn(move || client(port));

    let n = ioc.run();
    t.join().unwrap();

    assert_eq!(n, (1 + TOTAL_CONNS).into());
}

#[test]
fn tcp_connection_stress_test_cq_overflow() {
    const NR_FILES: u32 = 500;
    const CQ_ENTRIES: u32 = 512;

    const SERVER_BGID: u16 = 27;
    const CLIENT_BGID: u16 = 72;
    const MSG_LEN: usize = 1024;

    const TOTAL_CONNS: u32 = 3 * NR_FILES;

    fn make_io_context() -> fiona::IoContext {
        let params = &fiona::IoContextParams {
            sq_entries: 256,
            cq_entries: CQ_ENTRIES,
            nr_files: 2 * NR_FILES,
        };

        fiona::IoContext::with_params(params)
    }

    fn client(port: u16) {
        let mut ioc = make_io_context();
        let ex = ioc.get_executor();
        ex.register_buf_group(CLIENT_BGID, 4 * 1024, 1024).unwrap();

        ex.clone().spawn(async move {
            for _ in 0..(TOTAL_CONNS / NR_FILES) {
                let mut tasks: FuturesUnordered<fiona::SpawnFuture<()>> = (0..NR_FILES)
                    .map(|_idx| {
                        let ex2 = ex.clone();
                        ex.clone().spawn(async move {
                            let mut message = vec![0; MSG_LEN];
                            {
                                let mut rng = rand::rngs::StdRng::from_os_rng();
                                rand::RngCore::fill_bytes(&mut rng, &mut message);
                            }

                            let timer = fiona::time::Timer::new(ex2.clone());
                            timer
                                .wait(Duration::from_millis(message[0].into()))
                                .await
                                .unwrap();

                            let client = fiona::tcp::Client::new(ex2);
                            client
                                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                                .await
                                .unwrap();

                            let msg_copy = message.clone();
                            let message = client.send(message).await.unwrap();
                            assert!(message.is_empty());

                            client.set_buf_group(CLIENT_BGID);

                            let bufs = client.recv().await.unwrap();
                            let buf = flatten_bufs(bufs);

                            assert_eq!(buf, msg_copy);
                        })
                    })
                    .collect();

                while !tasks.is_empty() {
                    tasks.next().await.unwrap();
                }
            }
        });

        let n = ioc.run();
        assert_eq!(n, (1 + TOTAL_CONNS).into());
    }

    let mut ioc = make_io_context();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.clone().spawn(async move {
        ex.register_buf_group(SERVER_BGID, 4 * 1024, 1024).unwrap();

        let mut conns = 0;
        while conns < TOTAL_CONNS {
            let stream = acceptor.accept().await.unwrap();
            ex.clone().spawn(async move {
                stream.set_buf_group(SERVER_BGID);

                let bufs = stream.recv().await.unwrap();
                let send_buf = flatten_bufs(bufs);
                assert!(!send_buf.is_empty());

                let send_buf = stream.send(send_buf).await.unwrap();

                assert!(send_buf.is_empty());
            });
            conns += 1;
        }
    });

    let t = std::thread::spawn(move || client(port));

    let n = ioc.run();
    t.join().unwrap();

    assert_eq!(n, (1 + TOTAL_CONNS).into());
}

#[test]
fn tcp_double_connect() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.clone().spawn(async move {
        // let _stream = acceptor.accept().await.unwrap();
        let stream = acceptor.accept().await.unwrap();

        let timer = fiona::time::Timer::new(stream.get_executor());
        timer.wait(Duration::from_millis(250)).await.unwrap();
    });

    // sequential double-connect
    {
        let ex = ex.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex);
            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            let err = client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap_err();

            assert_eq!(err, Errno::from_raw(EISCONN));
        });
    }

    // concurrent double-connect
    {
        let ex = ex.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex);
            let mut f1 = client.connect_ipv4(Ipv4Addr::LOCALHOST, port);

            assert!(futures::poll!(&mut f1).is_pending());

            {
                let client = client.clone();
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        let _f2 = client.connect_ipv4(Ipv4Addr::LOCALHOST, port);
                    }))
                    .is_err()
                );
            }
        });
    }

    // concurrent double-connect when already connected
    {
        let ex = ex.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex);
            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            let mut f1 = client.connect_ipv4(Ipv4Addr::LOCALHOST, port);

            assert!(futures::poll!(&mut f1).is_pending());

            {
                let client = client.clone();
                assert!(
                    catch_unwind(AssertUnwindSafe(|| {
                        let _f2 = client.connect_ipv4(Ipv4Addr::LOCALHOST, port);
                    }))
                    .is_err()
                );
            }
        });
    }

    let n = ioc.run();
    assert!(n > 0);
}

#[test]
fn connect_select_ready_always() {
    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    let n = ioc.run();
    assert!(n > 0);
}
