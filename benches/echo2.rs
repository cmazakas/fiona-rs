#![allow(static_mut_refs)]

extern crate clap;
extern crate rand;
extern crate tokio;

mod utils;

use std::{
    hash::{DefaultHasher, Hasher},
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use clap::Parser;
use nix::{errno::Errno, libc::ENOBUFS};
use rand::SeedableRng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BUF_SIZE: usize = 4 * 1024 * 1024;
const RECV_BUF_SIZE: usize = 64 * 1024;
const SEND_BUF_SIZE: usize = 256 * 1024;
const NUM_BUFS: u32 = 16 * 1024;

const SEED: u64 = 1234;

static mut DURATION: Duration = Duration::new(0, 0);

const EXPECTED_HASH: u64 = 18157012549364724277;

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

    fn write(&mut self, bytes: &mut [u8; SEND_BUF_SIZE])
    {
        rand::RngCore::fill_bytes(&mut self.rng, bytes);
    }
}

fn tokio_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    static mut TIMINGS: Vec<Duration> = Vec::new();

    let rt = tokio::runtime::Builder::new_current_thread().enable_all()
                                                          .build()
                                                          .unwrap();

    rt.block_on(async {
          unsafe { DURATION = Duration::new(0, 0) };

          let start = Instant::now();
          let mut join_set = tokio::task::JoinSet::new();

          for _ in 0..nr_files {
              join_set.spawn(async move {
                          let start = Instant::now();

                          let mut generator = ByteGen::new();

                          let socket = tokio::net::TcpSocket::new_v4().unwrap();
                          let mut client = socket.connect(SocketAddr::new(IpAddr::V4(ipv4_addr),
                                                                          port))
                                                 .await
                                                 .unwrap();

                          let mut buf = [0; RECV_BUF_SIZE];

                          let mut total_received = 0;
                          let mut h = DefaultHasher::new();

                          let mut total_sent = 0;
                          let mut send_buf = Vec::<u8>::with_capacity(SEND_BUF_SIZE);

                          while total_sent < BUF_SIZE {
                              let mut chunk = [0_u8; SEND_BUF_SIZE];
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

                          while total_received < BUF_SIZE {
                              let n = tokio::time::timeout(Duration::from_secs(120),
                                                           client.read(&mut buf)).await
                                                                                 .unwrap()
                                                                                 .unwrap();

                              h.write(&buf[0..n]);
                              total_received += n;
                          }

                          let digest = h.finish();
                          assert_eq!(digest, EXPECTED_HASH);

                          unsafe { DURATION += start.elapsed() };
                          unsafe {
                              TIMINGS.push(start.elapsed());
                          }
                      });
          }
          join_set.join_all().await;
          let avg_dur = unsafe { DURATION / nr_files };
          println!("tokio average client duration: {avg_dur:?}");
          println!("tokio client loop took: {:?} for {nr_files} connections", start.elapsed());

          let mut outliers = 0;
          unsafe {
              for timing in &TIMINGS {
                  if *timing >= 2 * avg_dur {
                      outliers += 1;
                  }
              }
          }
          println!("total outliers: {outliers}");
      });

    Ok(())
}

fn tokio_echo_server(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    let rt = tokio::runtime::Builder::new_current_thread().enable_all()
                                                          .build()
                                                          .unwrap();

    rt.block_on(async {
          unsafe { DURATION = Duration::new(0, 0) };
          let start = Instant::now();

          let socket = tokio::net::TcpSocket::new_v4().unwrap();
          socket.reuseaddr().unwrap();
          socket.bind(SocketAddr::new(IpAddr::V4(ipv4_addr), port))
                .unwrap();

          let listener = socket.listen(2048).unwrap();

          let mut join_set = tokio::task::JoinSet::new();

          for _ in 0..nr_files {
              let mut stream = listener.accept().await.unwrap().0;

              join_set.spawn(async move {
                          let start = Instant::now();

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
                          assert_eq!(digest, EXPECTED_HASH);

                          let mut total_sent = 0;
                          let mut send_buf = Vec::<u8>::with_capacity(SEND_BUF_SIZE);

                          while total_sent < BUF_SIZE {
                              let mut chunk = [0_u8; SEND_BUF_SIZE];
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

                          unsafe { DURATION += start.elapsed() };
                      });
          }
          join_set.join_all().await;

          let avg_dur = unsafe { DURATION / nr_files };
          println!("average server duration: {avg_dur:?}");
          println!("tokio accept loop took: {:?} for {nr_files} connections", start.elapsed());
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

fn fiona_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    unsafe { DURATION = Duration::new(0, 0) };
    static mut TIMINGS: Vec<Duration> = Vec::new();

    let start = Instant::now();

    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    const CLIENT_BGID: u16 = 72;

    ex.register_buf_group(CLIENT_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    for _idx in 0..nr_files {
        let ex2 = ex.clone();
        ex.clone().spawn(async move {
                      let start = Instant::now();

                      let mut generator = ByteGen::new();

                      let mut total_received = 0;
                      let mut h = DefaultHasher::new();

                      let client = fiona::tcp::Client::new(ex2);
                      client.set_timeout(Duration::from_secs(120));
                      client.connect_ipv4(ipv4_addr, port).await.unwrap();

                      client.set_buf_group(CLIENT_BGID);

                      let mut total_sent = 0;
                      let mut send_buf = Vec::with_capacity(SEND_BUF_SIZE);
                      while total_sent < BUF_SIZE {
                          let mut chunk = [0_u8; SEND_BUF_SIZE];
                          generator.write(&mut chunk);
                          send_buf.extend_from_slice(&chunk);
                          while !send_buf.is_empty() {
                              let (num_sent, buf) = client.send(send_buf).await;
                              send_buf = buf;
                              let num_sent = num_sent.unwrap();
                              assert!(num_sent > 0);
                              drop(send_buf.drain(0..num_sent));
                              total_sent += num_sent;
                          }
                      }

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
                      assert_eq!(digest, EXPECTED_HASH);

                      unsafe { DURATION += start.elapsed() };
                      unsafe {
                          TIMINGS.push(start.elapsed());
                      }
                  });
    }

    let _n = ioc.run();
    let avg_dur = unsafe { DURATION / nr_files };
    println!("fiona average client duration: {avg_dur:?}");
    println!("fiona client loop took: {:?} for {nr_files} connections", start.elapsed());

    let mut outliers = 0;
    unsafe {
        for timing in &TIMINGS {
            if *timing >= 2 * avg_dur {
                outliers += 1;
            }
        }
    }
    println!("total outliers: {outliers}");

    Ok(())
}

fn fiona_echo_server(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    unsafe { DURATION = Duration::new(0, 0) };
    let start = Instant::now();

    const SERVER_BGID: u16 = 27;

    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::bind_ipv4(ex.clone(), ipv4_addr, port).unwrap();

    ex.register_buf_group(SERVER_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    ex.clone().spawn(async move {
                  for _idx in 0..nr_files {
                      let stream = acceptor.accept().await.unwrap();
                      ex.clone().spawn(async move {
                                    let start = Instant::now();

                                    let mut generator = ByteGen::new();

                                    stream.set_timeout(Duration::from_secs(120));
                                    stream.set_buf_group(SERVER_BGID);

                                    let mut total_received = 0;
                                    let mut h = DefaultHasher::new();

                                    while total_received < BUF_SIZE {
                                        match stream.recv().await {
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
                                    assert_eq!(digest, EXPECTED_HASH);

                                    let mut total_sent = 0;
                                    let mut send_buf = Vec::with_capacity(SEND_BUF_SIZE);
                                    while total_sent < BUF_SIZE {
                                        let mut chunk = [0_u8; SEND_BUF_SIZE];
                                        generator.write(&mut chunk);
                                        send_buf.extend_from_slice(&chunk);
                                        while !send_buf.is_empty() {
                                            let (num_sent, buf) = stream.send(send_buf).await;
                                            send_buf = buf;
                                            let num_sent = num_sent.unwrap();
                                            assert!(num_sent > 0);
                                            drop(send_buf.drain(0..num_sent));
                                            total_sent += num_sent;
                                        }
                                    }

                                    unsafe { DURATION += start.elapsed() };
                                });
                  }
              });

    let _n = ioc.run();

    let avg_dur = unsafe { DURATION / nr_files };
    println!("fiona average server duration: {avg_dur:?}");
    println!("fiona accept loop took: {:?} for {nr_files} connections", start.elapsed());

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

        assert_eq!(digest, EXPECTED_HASH);
    }

    if args.tokio {
        if args.server {
            utils::run_once("tokio echo2 server", || {
                black_box(tokio_echo_server(args.ipv4_addr, args.port, args.nr_files))
            }).unwrap();
        } else {
            assert!(args.client);
            utils::run_once("tokio echo2 client", || {
                black_box(tokio_echo_client(args.ipv4_addr, args.port, args.nr_files))
            }).unwrap();
        }
    } else if args.fiona {
        if args.server {
            utils::run_once("fiona echo2 server", || {
                black_box(fiona_echo_server(args.ipv4_addr, args.port, args.nr_files))
            }).unwrap();
        } else {
            assert!(args.client);
            utils::run_once("fiona echo2 client", || {
                black_box(fiona_echo_client(args.ipv4_addr, args.port, args.nr_files))
            }).unwrap();
        }
    }
}
