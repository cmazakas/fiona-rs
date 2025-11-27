#![allow(static_mut_refs)]

extern crate clap;
extern crate rand;
extern crate tokio;

mod utils;

use std::{
    hash::{DefaultHasher, Hasher},
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicU64, Ordering},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use clap::Parser;
use fiona::tcp::AcceptorOpts;
use nix::{errno::Errno, libc::ENOBUFS};
use rand::SeedableRng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BUF_SIZE: usize = 256 * 1024;
const RECV_BUF_SIZE: usize = 8 * 1024;
const NUM_BUFS: u32 = 32 * 1024;
const CLIENT_BGID: u16 = 72;
const SERVER_BGID: u16 = 27;

const SEED: u64 = 1234;

fn make_bytes() -> Vec<u8>
{
    let mut rng = rand::rngs::StdRng::seed_from_u64(SEED);
    let mut bytes = vec![0_u8; BUF_SIZE];
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
}

struct ByteGen
{
    rng: rand::rngs::StdRng,
}

impl ByteGen
{
    fn new() -> Self
    {
        Self { rng: rand::rngs::StdRng::seed_from_u64(SEED) }
    }

    fn write(&mut self, bytes: &mut [u8; 16 * 1024])
    {
        rand::RngCore::fill_bytes(&mut self.rng, bytes);
    }
}

fn tokio_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, nr_threads: u32)
                     -> Result<(), String>
{
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(nr_threads.try_into()
                                                                                  .unwrap())
                                                        .enable_all()
                                                        .build()
                                                        .unwrap();

    rt.block_on(async {
          let start = Instant::now();
          let mut join_set = tokio::task::JoinSet::new();

          for _ in 0..nr_threads * nr_files {
              join_set.spawn(async move {
                          let mut generator = ByteGen::new();

                          let socket = tokio::net::TcpSocket::new_v4().unwrap();
                          let mut client = socket.connect(SocketAddr::new(IpAddr::V4(ipv4_addr),
                                                                          port))
                                                 .await
                                                 .unwrap();

                          let mut total_sent = 0;
                          let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                          while total_sent < BUF_SIZE {
                              let mut chunk = [0_u8; 16 * 1024];
                              generator.write(&mut chunk);

                              send_buf.extend_from_slice(&chunk);
                              while !send_buf.is_empty() {
                                  let n = tokio::time::timeout(Duration::from_secs(120),
                                                               client.write(&send_buf)).await
                                                                                       .unwrap()
                                                                                       .unwrap();

                                  assert!(n > 0);

                                  drop(send_buf.drain(0..n));
                                  total_sent += n;
                              }
                          }

                          let mut buf = [0; RECV_BUF_SIZE];

                          let mut total_received = 0;
                          let mut h = DefaultHasher::new();

                          while total_received < BUF_SIZE {
                              let n = tokio::time::timeout(Duration::from_secs(120),
                                                           client.read(&mut buf)).await
                                                                                 .unwrap()
                                                                                 .unwrap();

                              h.write(&buf[0..n]);
                              total_received += n;
                          }

                          let digest = h.finish();
                          assert_eq!(digest, 5326650159322985034);
                      });
          }
          join_set.join_all().await;

          println!("tokio client loop took: {:?} for {} connections",
                   start.elapsed(),
                   nr_files * nr_threads);
      });

    Ok(())
}

fn tokio_echo_server(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, nr_threads: u32)
                     -> Result<(), String>
{
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(nr_threads.try_into()
                                                                                  .unwrap())
                                                        .enable_all()
                                                        .build()
                                                        .unwrap();

    rt.block_on(async {
          let start = Instant::now();

          let socket = tokio::net::TcpSocket::new_v4().unwrap();
          socket.reuseaddr().unwrap();
          socket.bind(SocketAddr::new(IpAddr::V4(ipv4_addr), port))
                .unwrap();

          let listener = socket.listen(2048).unwrap();

          let mut join_set = tokio::task::JoinSet::new();

          for _ in 0..nr_threads * nr_files {
              let mut stream = listener.accept().await.unwrap().0;

              join_set.spawn(async move {
                          let mut generator = ByteGen::new();

                          let mut buf = [0; RECV_BUF_SIZE];

                          let mut total_received = 0;
                          let mut h = DefaultHasher::new();

                          while total_received < BUF_SIZE {
                              let n = tokio::time::timeout(Duration::from_secs(120),
                                                           stream.read(&mut buf)).await
                                                                                 .unwrap()
                                                                                 .unwrap();

                              h.write(&buf[0..n]);
                              total_received += n;
                          }

                          let digest = h.finish();
                          assert_eq!(digest, 5326650159322985034);

                          let mut total_sent = 0;
                          let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                          while total_sent < BUF_SIZE {
                              let mut chunk = [0_u8; 16 * 1024];
                              generator.write(&mut chunk);

                              send_buf.extend_from_slice(&chunk);
                              while !send_buf.is_empty() {
                                  let n = tokio::time::timeout(Duration::from_secs(120),
                                                               stream.write(&send_buf)).await
                                                                                       .unwrap()
                                                                                       .unwrap();

                                  assert!(n > 0);

                                  drop(send_buf.drain(0..n));
                                  total_sent += n;
                              }
                          }
                      });
          }
          join_set.join_all().await;

          println!("tokio accept loop took: {:?} for {} connections",
                   start.elapsed(),
                   nr_files * nr_threads);
      });

    Ok(())
}

const CQ_ENTRIES: u32 = 64 * 1024;

fn make_io_context(nr_files: u32) -> fiona::IoContext
{
    let params = &fiona::IoContextParams { sq_entries: 256,
                                           cq_entries: CQ_ENTRIES,
                                           nr_files: 2 * nr_files };

    fiona::IoContext::with_params(params)
}

async fn fiona_echo_client_impl(ex: fiona::Executor, ipv4_addr: Ipv4Addr, port: u16)
{
    let mut generator = ByteGen::new();

    let client = fiona::tcp::Client::new(ex);
    client.set_timeout(Duration::from_secs(120));
    client.connect_ipv4(ipv4_addr, port).await.unwrap();

    client.set_buf_group(CLIENT_BGID);

    let mut send_buf = Vec::with_capacity(16 * 1024);

    let mut total_sent = 0;
    while total_sent < BUF_SIZE {
        let mut chunk = [0_u8; 16 * 1024];
        generator.write(&mut chunk);

        send_buf.extend_from_slice(&chunk);
        let (num_sent, mut buf) = client.send(send_buf).await;
        assert_eq!(num_sent.unwrap(), buf.len());
        buf.clear();

        send_buf = buf;
        total_sent += num_sent.unwrap();
    }

    let mut total_received = 0;
    let mut h = DefaultHasher::new();

    while total_received < BUF_SIZE {
        let mbufs = client.recv().await;
        match mbufs {
            Ok(bufs) => {
                for buf in &bufs {
                    if buf.is_empty() {
                        assert_eq!(total_received, BUF_SIZE);
                    } else {
                        h.write(buf);
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

    let digest = h.finish();
    assert_eq!(digest, 5326650159322985034);
}

fn fiona_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, nr_threads: u32)
                     -> Result<(), String>
{
    let start = Instant::now();

    let mut threads = Vec::<JoinHandle<()>>::with_capacity(nr_threads as _);
    for _ in 0..nr_threads {
        threads.push(std::thread::spawn(move || {
                         let mut ioc = make_io_context(nr_files);
                         let ex = ioc.get_executor();

                         ex.register_buf_group(CLIENT_BGID, NUM_BUFS, RECV_BUF_SIZE)
                           .unwrap();

                         for _ in 0..nr_files {
                             ex.spawn(fiona_echo_client_impl(ex.clone(), ipv4_addr, port));
                         }

                         ioc.run();
                     }));
    }

    for t in threads {
        t.join().unwrap();
    }

    println!("fiona client loop took: {:?} for {} connections",
             start.elapsed(),
             nr_files * nr_threads);

    Ok(())
}

async fn fiona_echo_server_impl(stream: fiona::tcp::Stream)
{
    stream.set_timeout(Duration::from_secs(120));
    stream.set_buf_group(SERVER_BGID);

    let mut total_received = 0;
    let mut h = DefaultHasher::new();

    while total_received < BUF_SIZE {
        let mbufs = stream.recv().await;
        match mbufs {
            Ok(bufs) => {
                for buf in &bufs {
                    if buf.is_empty() {
                        assert_eq!(total_received, BUF_SIZE);
                    } else {
                        h.write(buf);
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

    let digest = h.finish();
    assert_eq!(digest, 5326650159322985034);

    let mut generator = ByteGen::new();
    let mut send_buf = Vec::with_capacity(16 * 1024);

    let mut total_sent = 0;
    while total_sent < BUF_SIZE {
        let mut chunk = [0_u8; 16 * 1024];
        generator.write(&mut chunk);

        send_buf.extend_from_slice(&chunk);
        let (num_sent, mut buf) = stream.send(send_buf).await;
        assert_eq!(num_sent.unwrap(), buf.len());
        buf.clear();

        send_buf = buf;
        total_sent += num_sent.unwrap();
    }
}

fn fiona_echo_server(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32, nr_threads: u32)
                     -> Result<(), String>
{
    static TOTAL_CONNS: AtomicU64 = AtomicU64::new(0);
    TOTAL_CONNS.store(nr_threads as u64 * nr_files as u64, Ordering::Relaxed);

    let mut threads = Vec::<JoinHandle<()>>::with_capacity(nr_threads as _);
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(16);

    let start = Instant::now();
    for _ in 0..nr_threads {
        let (shutdown_tx, mut shutdown_rx) = (shutdown_tx.clone(), shutdown_tx.subscribe());
        threads.push(std::thread::spawn(move || {
                         let mut ioc = make_io_context(nr_files);
                         let ex = ioc.get_executor();

                         ex.register_buf_group(SERVER_BGID, NUM_BUFS, RECV_BUF_SIZE)
                           .unwrap();

                         let opts = &AcceptorOpts { reuse_addr: true,
                                                    reuse_port: true };
                         let acceptor = fiona::tcp::Acceptor::bind_ipv4_with_params(ex.clone(),
                                                                                    ipv4_addr,
                                                                                    port,
                                                                                    opts).unwrap();

                         ex //
                           .clone()
                           .spawn(async move {
                               loop {
                                   tokio::select! {
                                       stream = acceptor.accept() => {
                                        ex.spawn(fiona_echo_server_impl(stream.unwrap()));

                                           if TOTAL_CONNS.fetch_sub(1, Ordering::Relaxed) == 1 {
                                               shutdown_tx.send(()).unwrap();
                                               break;
                                           }
                                       },
                                    _ = shutdown_rx.recv() => {
                                        break;
                                    },
                                   }
                               }
                           });

                         ioc.run();
                     }));
    }

    for t in threads {
        t.join().unwrap();
    }

    println!("fiona accept loop took: {:?} for {} connections",
             start.elapsed(),
             nr_threads * nr_files);

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CliArgs
{
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
    nr_files: u32,

    #[arg(long, default_value_t = 16)]
    nr_threads: u32,
}

fn main()
{
    let args = CliArgs::parse();
    // println!("{args:?}");

    {
        let bytes = make_bytes();

        let mut h = DefaultHasher::new();
        h.write(&bytes);
        let digest = h.finish();

        assert_eq!(digest, 5326650159322985034);

        let mut h2 = DefaultHasher::new();
        let mut generator = ByteGen::new();
        let mut buf = [0_u8; 16 * 1024];

        for _ in 0..(BUF_SIZE / (16 * 1024)) {
            generator.write(&mut buf);
            h2.write(&buf);
        }

        assert_eq!(h2.finish(), h.finish());
    }

    if args.tokio {
        if args.server {
            utils::run_once("tokio echo2 server", || {
                black_box(tokio_echo_server(args.ipv4_addr,
                                            args.port,
                                            args.nr_files,
                                            args.nr_threads))
            }).unwrap();
        } else {
            assert!(args.client);
            utils::run_once("tokio echo2 client", || {
                black_box(tokio_echo_client(args.ipv4_addr,
                                            args.port,
                                            args.nr_files,
                                            args.nr_threads))
            }).unwrap();
        }
    } else if args.fiona {
        if args.server {
            utils::run_once("fiona echo2 server", || {
                black_box(fiona_echo_server(args.ipv4_addr,
                                            args.port,
                                            args.nr_files,
                                            args.nr_threads))
            }).unwrap();
        } else {
            assert!(args.client);
            utils::run_once("fiona echo2 client", || {
                black_box(fiona_echo_client(args.ipv4_addr,
                                            args.port,
                                            args.nr_files,
                                            args.nr_threads))
            }).unwrap();
        }
    }
}
