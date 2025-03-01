extern crate rand;
extern crate tokio;

mod utils;

use std::{net::Ipv4Addr, sync::atomic::AtomicU64, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NR_FILES: u32 = 5000;
const NUM_MSGS: u32 = 1000;
const CQ_ENTRIES: u32 = 64 * 1024;

const SERVER_BGID: u16 = 27;
const CLIENT_BGID: u16 = 72;
// const MSG_LEN: usize = 4096;

static NUM_RUNS: AtomicU64 = AtomicU64::new(0);

fn make_io_context() -> fiona::IoContext {
    let params = &fiona::IoContextParams {
        sq_entries: 256,
        cq_entries: CQ_ENTRIES,
        nr_files: 4 * NR_FILES,
    };

    fiona::IoContext::with_params(params)
}

fn client(port: u16) {
    let mut ioc = make_io_context();
    let ex = ioc.get_executor();
    ex.register_buf_group(CLIENT_BGID, 16 * 1024, 1024).unwrap();

    for _ in 0..NR_FILES {
        let ex2 = ex.clone();
        ex.clone().spawn(async move {
            let client = fiona::tcp::Client::new(ex2);
            client
                .connect_ipv4(Ipv4Addr::LOCALHOST, port)
                .await
                .unwrap();

            client.set_buf_group(CLIENT_BGID);

            for _ in 0..NUM_MSGS {
                let message = String::from("hello, world!").into_bytes();
                let message = client.send(message).await.unwrap();
                assert!(message.is_empty());
                let bufs = client.recv().await.unwrap();
                assert_eq!(bufs[0], "hello, world!".as_bytes());
            }

            NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    let _n = ioc.run();
}

fn fiona_echo() -> Result<(), String> {
    let mut ioc = make_io_context();
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    let port = acceptor.port();

    ex.clone().spawn(async move {
        ex.register_buf_group(SERVER_BGID, 16 * 1024, 1024).unwrap();

        for _ in 0..NR_FILES {
            let stream = acceptor.accept().await.unwrap();
            ex.clone().spawn(async move {
                stream.set_buf_group(SERVER_BGID);

                for _ in 0..NUM_MSGS {
                    let bufs = stream.recv().await.unwrap();
                    assert_eq!(bufs[0], "hello, world!".as_bytes());

                    let message = String::from("hello, world!").into_bytes();

                    let message = stream.send(message).await.unwrap();

                    assert!(message.is_empty());
                }

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            });
        }
    });

    let t = std::thread::spawn(move || client(port));

    let _n = ioc.run();
    t.join().unwrap();

    Ok(())
}

fn tokio_echo() -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let t = std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let mut join_set = tokio::task::JoinSet::new();
            for _ in 0..NR_FILES {
                join_set.spawn(async {
                    let addr = "127.0.0.1:8080".parse().unwrap();

                    let socket = tokio::net::TcpSocket::new_v4().unwrap();
                    let mut client = socket.connect(addr).await.unwrap();

                    for _ in 0..NUM_MSGS {
                        let message = String::from("hello, world!").into_bytes();
                        let n =
                            tokio::time::timeout(Duration::from_secs(3), client.write(&message))
                                .await
                                .unwrap()
                                .unwrap();

                        assert_eq!(n, 13);

                        let mut buf = [0; 1024];
                        let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
                            .await
                            .unwrap()
                            .unwrap();

                        assert_eq!(&buf[0..n], "hello, world!".as_bytes());
                    }
                });

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            join_set.join_all().await;
        });
    });

    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
            .await
            .unwrap();

        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..NR_FILES {
            let mut stream = listener.accept().await.unwrap().0;

            join_set.spawn(async move {
                for _ in 0..NUM_MSGS {
                    let mut buf = [0; 1024];
                    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();

                    assert_eq!(&buf[0..n], "hello, world!".as_bytes());

                    let n = tokio::time::timeout(
                        Duration::from_secs(3),
                        stream.write("hello, world!".as_bytes()),
                    )
                    .await
                    .unwrap()
                    .unwrap();

                    assert_eq!(n, 13);
                }

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            });
        }
        join_set.join_all().await;
    });

    t.join().unwrap();

    Ok(())
}

fn main() {
    utils::run_once("fiona_echo", fiona_echo).unwrap();
    utils::run_once("tokio_echo", tokio_echo).unwrap();
}
