// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![allow(static_mut_refs)]

extern crate blake2;
extern crate clap;
extern crate rand;
extern crate tokio;

mod utils;

use std::{
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, atomic::AtomicU32},
    thread::{JoinHandle, spawn},
    time::{Duration, Instant},
};

use blake2::Digest;
use clap::Parser;
use nix::{
    errno::Errno,
    libc::{ENOBUFS, SHUT_WR},
};
use rand::SeedableRng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const BUF_SIZE: usize = 1024 * 1024;
const RECV_BUF_SIZE: usize = 8 * 1024;
const SEND_BUF_SIZE: usize = 16 * 1024;
const NUM_BUFS: u32 = 16 * 1024;
const CQ_ENTRIES: u32 = 64 * 1024;
const SEED: u64 = 1234;

const EXPECTED_HASH: &[u8] = &[
    162, 235, 8, 253, 143, 97, 130, 112, 211, 231, 203, 40, 111, 223, 250, 216, 32, 207, 52, 92,
    230, 249, 226, 171, 109, 167, 186, 135, 68, 179, 92, 185, 104, 208, 197, 220, 235, 16, 132,
    188, 195, 142, 53, 212, 207, 81, 192, 105, 109, 102, 169, 224, 46, 22, 187, 175, 107, 189, 182,
    22, 35, 25, 231, 51,
];

fn make_bytes() -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    let mut bytes = vec![0_u8; BUF_SIZE];
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
}

async fn tokio_send(stream: &mut TcpStream, bytes: Arc<Vec<u8>>) {
    let mut total_sent = 0;
    let mut send_buf = vec![0_u8; SEND_BUF_SIZE];

    while total_sent < BUF_SIZE {
        send_buf.copy_from_slice(&bytes[total_sent..total_sent + SEND_BUF_SIZE]);

        let mut n = 0;
        while n < SEND_BUF_SIZE {
            let sent = timeout(Duration::from_secs(120), stream.write(&send_buf[n..]))
                .await
                .unwrap()
                .unwrap();

            assert!(sent > 0);
            n += sent;
            total_sent += sent;
        }
    }
}

async fn tokio_recv(h: &mut blake2::Blake2b512, stream: &mut TcpStream) {
    let mut buf = vec![0; RECV_BUF_SIZE];

    let mut total_received = 0;
    while total_received < BUF_SIZE {
        let n = timeout(Duration::from_secs(120), stream.read(&mut buf))
            .await
            .unwrap()
            .unwrap();

        h.update(&buf[0..n]);
        total_received += n;
    }
}

async fn tokio_close(stream: &mut TcpStream) {
    stream.shutdown().await.unwrap();
}

fn tokio_echo_client(
    ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, num_threads: u32, bytes: Vec<u8>,
) -> Result<(), String> {
    let bytes = Arc::new(bytes);

    let start = Instant::now();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads as _)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..nr_files * num_threads {
            let bytes = bytes.clone();
            join_set.spawn(async move {
                let mut h = blake2::Blake2b512::new();

                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                let mut client = socket
                    .connect(SocketAddr::new(IpAddr::V4(ipv4_addr), port))
                    .await
                    .unwrap();

                tokio_send(&mut client, bytes).await;
                tokio_recv(&mut h, &mut client).await;

                let digest = h.finalize();
                assert_eq!(digest.as_slice(), EXPECTED_HASH);

                tokio_close(&mut client).await;
                drop(client);
            });
        }

        join_set.join_all().await;
    });

    println!(
        "tokio client loop took: {:?} for {} connections",
        start.elapsed(),
        nr_files * num_threads
    );

    Ok(())
}

fn tokio_echo_server(
    ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, num_threads: u32, bytes: Vec<u8>,
) -> Result<(), String> {
    let bytes = Arc::new(bytes);
    let total_conns = Arc::new(AtomicU32::new(nr_files * num_threads));
    let cancel_token = tokio_util::sync::CancellationToken::new();

    let start = Instant::now();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_threads as _)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let mut join_set = tokio::task::JoinSet::new();

        for _ in 0..num_threads {
            let bytes = bytes.clone();
            let total_conns = total_conns.clone();
            let cancel_token = cancel_token.clone();

            join_set.spawn(async move {
                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                socket.set_reuseaddr(true).unwrap();
                socket.set_reuseport(true).unwrap();
                socket
                    .bind(SocketAddr::new(IpAddr::V4(ipv4_addr), port))
                    .unwrap();

                let listener = socket.listen(2048).unwrap();

                let mut join_set = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        res = listener.accept() => {
                            let mut stream = res.unwrap().0;
                            let bytes = bytes.clone();
                            join_set.spawn(async move {
                                let mut h = blake2::Blake2b512::new();

                                tokio_recv(&mut h, &mut stream).await;
                                let digest = h.finalize();
                                assert_eq!(digest.as_slice(), EXPECTED_HASH);

                                tokio_send(&mut stream, bytes).await;

                                tokio_close(&mut stream).await;
                                drop(stream);
                            });
                        },
                        _ = cancel_token.cancelled() => {
                            break;
                        },
                    }

                    let n = total_conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    if n == 1 {
                        cancel_token.cancel();
                    }
                }
                join_set.join_all().await;
            });
        }

        join_set.join_all().await;
    });

    println!(
        "tokio accept loop took: {:?} for {} connections",
        start.elapsed(),
        nr_files * num_threads
    );

    Ok(())
}

async fn fiona_send(stream: fiona::net::TcpStream, bytes: Arc<Vec<u8>>) {
    let mut total_sent = 0;
    let mut send_buf = vec![0_u8; SEND_BUF_SIZE];

    while total_sent < BUF_SIZE {
        send_buf.copy_from_slice(&bytes[total_sent..total_sent + SEND_BUF_SIZE]);

        let mut n = 0;
        while n < SEND_BUF_SIZE {
            let (num_sent, buf) = stream.send_subspan(n.., send_buf).await;
            let num_sent = num_sent.unwrap();
            assert!(num_sent > 0);

            n += num_sent;
            total_sent += num_sent;

            send_buf = buf;
        }
    }
}

async fn fiona_recv(h: &mut blake2::Blake2b512, stream: fiona::net::TcpStream) {
    let mut total_received = 0;
    while total_received < BUF_SIZE {
        let mbufs = stream.recv().await;
        match mbufs {
            Ok(bufs) => {
                for buf in &bufs {
                    if buf.is_empty() {
                        assert_eq!(total_received, BUF_SIZE);
                    } else {
                        h.update(buf);
                        total_received += buf.len();
                    }
                }
            }
            Err(x) if x == Errno::from_raw(ENOBUFS) => {
                continue;
            }
            Err(err) => panic!("{err:?}"),
        }
    }
}

async fn fiona_close(stream: fiona::net::TcpStream) {
    stream.shutdown(SHUT_WR).await.unwrap();
    stream.close().await.unwrap();
}

fn make_io_context(nr_files: u32) -> fiona::IoContext {
    fiona::IoContext::builder()
        .sq_entries(256)
        .cq_entries(CQ_ENTRIES)
        .num_files(2 * nr_files)
        .build()
}

fn fiona_echo_client(
    ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, num_threads: u32, bytes: Vec<u8>,
) -> Result<(), String> {
    const CLIENT_BGID: u16 = 72;

    let bytes = Arc::new(bytes);
    let start = Instant::now();

    let mut threads = Vec::<JoinHandle<()>>::new();
    for _ in 0..num_threads {
        let bytes = bytes.clone();

        threads.push(spawn(move || {
            let mut ioc = make_io_context(nr_files);
            let ex = ioc.get_executor();

            ex.register_buf_group(CLIENT_BGID, NUM_BUFS, RECV_BUF_SIZE)
                .unwrap();

            for _ in 0..nr_files {
                let ex2 = ex.clone();
                let bytes = bytes.clone();
                ex.clone().spawn(async move {
                    let mut h = blake2::Blake2b512::new();

                    let stream = fiona::net::TcpClient::new(&ex2)
                        .with_timeout(Duration::from_secs(10))
                        .connect_ipv4(ipv4_addr, port)
                        .await
                        .unwrap();

                    stream.set_buf_group(CLIENT_BGID);
                    stream.set_timeout(Duration::from_secs(120));

                    fiona_send(stream.clone(), bytes.clone()).await;
                    fiona_recv(&mut h, stream.clone()).await;

                    let digest = h.finalize();
                    assert_eq!(digest.as_slice(), EXPECTED_HASH);

                    fiona_close(stream.clone()).await;
                    drop(stream);
                });
            }

            let _n = ioc.run();
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    println!(
        "fiona client loop took: {:?} for {} connections",
        start.elapsed(),
        nr_files as i32 * num_threads as i32
    );

    Ok(())
}

fn fiona_echo_server(
    ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, num_threads: u32, bytes: Vec<u8>,
) -> Result<(), String> {
    const SERVER_BGID: u16 = 27;

    let bytes = Arc::new(bytes);
    let cancel_token = tokio_util::sync::CancellationToken::new();
    let total_conns = Arc::new(AtomicU32::new(nr_files * num_threads));
    let mut threads = Vec::<JoinHandle<()>>::new();

    let start = Instant::now();

    for _ in 0..num_threads {
        let bytes = bytes.clone();
        let total_conns = total_conns.clone();
        let cancel_token = cancel_token.clone();

        threads.push(spawn(move || {
            let mut ioc = make_io_context(nr_files);
            let ex = ioc.get_executor();

            let opts = fiona::net::TcpListenerOpts {
                reuse_port: true,
                reuse_addr: true,
            };

            let acceptor =
                fiona::net::TcpListener::bind_ipv4_with_params(&ex, ipv4_addr, port, &opts)
                    .unwrap();

            ex.register_buf_group(SERVER_BGID, NUM_BUFS, RECV_BUF_SIZE)
                .unwrap();

            {
                let acceptor = acceptor.clone();
                let cancel_token = cancel_token.clone();
                ex.clone().spawn(async move {
                    cancel_token.cancelled().await;
                    acceptor.cancel().await.unwrap();
                    acceptor.close().await.unwrap();
                });
            }

            ex.clone().spawn(async move {
                loop {
                    let stream = acceptor.accept().await;
                    if let Err(err) = stream {
                        assert_eq!(err, Errno::ECANCELED);
                        break;
                    }

                    let stream = stream.unwrap();
                    let bytes = bytes.clone();

                    ex.clone().spawn(async move {
                        let mut h = blake2::Blake2b512::new();

                        stream.set_timeout(Duration::from_secs(120));
                        stream.set_buf_group(SERVER_BGID);

                        fiona_recv(&mut h, stream.clone()).await;
                        let digest = h.finalize();
                        assert_eq!(digest.as_slice(), EXPECTED_HASH);

                        fiona_send(stream.clone(), bytes).await;

                        fiona_close(stream.clone()).await;
                        drop(stream);
                    });

                    let n = total_conns.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    if n == 1 {
                        cancel_token.cancel();
                    }
                }
            });

            let _n = ioc.run();
        }));
    }

    for t in threads {
        t.join().unwrap();
    }

    println!(
        "fiona accept loop took: {:?} for {} connections",
        start.elapsed(),
        nr_files as i32 * num_threads as i32
    );

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliArgs {
    #[arg(long)]
    tokio: bool,

    #[arg(long, conflicts_with = "tokio")]
    fiona: bool,

    #[arg(long, conflicts_with = "tokio", conflicts_with = "fiona")]
    liburing_rs: bool,

    #[arg(long)]
    bench: bool,

    #[arg(long)]
    ipv4_addr: Ipv4Addr,

    #[arg(long)]
    port: u16,

    #[arg(long)]
    server: bool,

    #[arg(long, conflicts_with = "server")]
    client: bool,

    #[arg(long, default_value_t = 5000)]
    num_files: u32,

    #[arg(long, default_value_t = 16)]
    num_threads: u32,
}

fn main() {
    let args = CliArgs::parse();
    // println!("{args:?}");

    let bytes = make_bytes();

    let mut h = blake2::Blake2b512::new();
    h.update(&bytes);
    let digest = h.finalize();

    assert_eq!(digest.as_slice(), EXPECTED_HASH);

    if args.tokio {
        if args.server {
            utils::run_once("tokio echo2 server", || {
                black_box(tokio_echo_server(
                    args.ipv4_addr,
                    args.port,
                    args.num_files,
                    args.num_threads,
                    bytes,
                ))
            })
            .unwrap();
        } else {
            assert!(args.client);
            utils::run_once("tokio echo2 client", || {
                black_box(tokio_echo_client(
                    args.ipv4_addr,
                    args.port,
                    args.num_files,
                    args.num_threads,
                    bytes,
                ))
            })
            .unwrap();
        }
    } else if args.fiona {
        if args.server {
            utils::run_once("fiona echo2 server", move || {
                black_box(fiona_echo_server(
                    args.ipv4_addr,
                    args.port,
                    args.num_files,
                    args.num_threads,
                    bytes,
                ))
            })
            .unwrap();
        } else {
            assert!(args.client);
            utils::run_once("fiona echo2 client", || {
                black_box(fiona_echo_client(
                    args.ipv4_addr,
                    args.port,
                    args.num_files,
                    args.num_threads,
                    bytes,
                ))
            })
            .unwrap();
        }
    }
}
