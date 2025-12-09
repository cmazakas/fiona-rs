#![allow(static_mut_refs)]

extern crate blake2;
extern crate clap;
extern crate rand;
extern crate tokio;

mod utils;

use std::{
    hint::black_box,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use blake2::Digest;
use clap::Parser;
use nix::{errno::Errno, libc::ENOBUFS};
use rand::SeedableRng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

const BUF_SIZE: usize = 1024 * 1024;
const RECV_BUF_SIZE: usize = 64 * 1024;
const SEND_BUF_SIZE: usize = 256 * 1024;
const NUM_BUFS: u32 = 16 * 1024;
const CQ_ENTRIES: u32 = 64 * 1024;
const SEED: u64 = 1234;

const EXPECTED_HASH: &[u8] = &[162, 235, 8, 253, 143, 97, 130, 112, 211, 231, 203, 40, 111, 223,
                               250, 216, 32, 207, 52, 92, 230, 249, 226, 171, 109, 167, 186, 135,
                               68, 179, 92, 185, 104, 208, 197, 220, 235, 16, 132, 188, 195, 142,
                               53, 212, 207, 81, 192, 105, 109, 102, 169, 224, 46, 22, 187, 175,
                               107, 189, 182, 22, 35, 25, 231, 51];

static mut DURATION: Duration = Duration::new(0, 0);

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

    fn write(&mut self, bytes: &mut [u8])
    {
        rand::RngCore::fill_bytes(&mut self.rng, bytes);
    }
}

async fn tokio_send(generator: &mut ByteGen, stream: &mut TcpStream)
{
    let mut total_sent = 0;
    let mut send_buf = vec![0_u8; SEND_BUF_SIZE];

    while total_sent < BUF_SIZE {
        generator.write(send_buf.as_mut_slice());

        let mut n = 0;
        while n < SEND_BUF_SIZE {
            let sent = timeout(Duration::from_secs(120), stream.write(&send_buf[n..])).await
                                                                                      .unwrap()
                                                                                      .unwrap();

            assert!(sent > 0);
            n += sent;
            total_sent += sent;
        }
    }
}

async fn tokio_recv(h: &mut blake2::Blake2b512, stream: &mut TcpStream)
{
    let mut buf = vec![0; RECV_BUF_SIZE];

    let mut total_received = 0;
    while total_received < BUF_SIZE {
        let n = timeout(Duration::from_secs(120), stream.read(&mut buf)).await
                                                                        .unwrap()
                                                                        .unwrap();

        h.update(&buf[0..n]);
        total_received += n;
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

                          let mut h = blake2::Blake2b512::new();
                          let mut generator = ByteGen::new();

                          let socket = tokio::net::TcpSocket::new_v4().unwrap();
                          let mut client = socket.connect(SocketAddr::new(IpAddr::V4(ipv4_addr),
                                                                          port))
                                                 .await
                                                 .unwrap();

                          tokio_send(&mut generator, &mut client).await;
                          tokio_recv(&mut h, &mut client).await;

                          let digest = h.finalize();
                          assert_eq!(digest.as_slice(), EXPECTED_HASH);

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

                          let mut h = blake2::Blake2b512::new();
                          let mut generator = ByteGen::new();

                          tokio_recv(&mut h, &mut stream).await;
                          let digest = h.finalize();
                          assert_eq!(digest.as_slice(), EXPECTED_HASH);

                          tokio_send(&mut generator, &mut stream).await;

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

fn make_io_context(nr_files: u32) -> fiona::IoContext
{
    let params = &fiona::IoContextParams { sq_entries: 256,
                                           cq_entries: CQ_ENTRIES,
                                           nr_files: 2 * nr_files };

    fiona::IoContext::with_params(params)
}

async fn fiona_send(generator: &mut ByteGen, stream: fiona::tcp::Stream)
{
    let mut total_sent = 0;
    let mut send_buf = vec![0_u8; SEND_BUF_SIZE];

    while total_sent < BUF_SIZE {
        generator.write(send_buf.as_mut_slice());

        let mut n = 0;
        while n < SEND_BUF_SIZE {
            let (num_sent, buf) = stream.send_subspan(n..send_buf.len(), send_buf).await;
            let num_sent = num_sent.unwrap();
            assert!(num_sent > 0);

            n += num_sent;
            total_sent += num_sent;

            send_buf = buf;
        }
    }
}

async fn fiona_recv(h: &mut blake2::Blake2b512, stream: fiona::tcp::Stream)
{
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

fn fiona_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    static mut TIMINGS: Vec<Duration> = Vec::new();

    let start = Instant::now();

    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    const CLIENT_BGID: u16 = 72;

    ex.register_buf_group(CLIENT_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    unsafe { DURATION = Duration::new(0, 0) };
    for _ in 0..nr_files {
        let ex2 = ex.clone();
        ex.clone().spawn(async move {
                      let start = Instant::now();

                      let mut h = blake2::Blake2b512::new();
                      let mut generator = ByteGen::new();

                      let client = fiona::tcp::Client::new(ex2);
                      client.set_buf_group(CLIENT_BGID);
                      client.set_timeout(Duration::from_secs(120));
                      client.connect_ipv4(ipv4_addr, port).await.unwrap();

                      fiona_send(&mut generator, client.as_stream()).await;
                      fiona_recv(&mut h, client.as_stream()).await;

                      let digest = h.finalize();
                      assert_eq!(digest.as_slice(), EXPECTED_HASH);

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
    let start = Instant::now();

    const SERVER_BGID: u16 = 27;

    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    let acceptor = fiona::tcp::Acceptor::bind_ipv4(ex.clone(), ipv4_addr, port).unwrap();

    ex.register_buf_group(SERVER_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    unsafe { DURATION = Duration::new(0, 0) };
    ex.clone().spawn(async move {
                  for _ in 0..nr_files {
                      let stream = acceptor.accept().await.unwrap();
                      ex.clone().spawn(async move {
                                    let start = Instant::now();

                                    let mut h = blake2::Blake2b512::new();
                                    let mut generator = ByteGen::new();

                                    stream.set_timeout(Duration::from_secs(120));
                                    stream.set_buf_group(SERVER_BGID);

                                    fiona_recv(&mut h, stream.clone()).await;
                                    let digest = h.finalize();
                                    assert_eq!(digest.as_slice(), EXPECTED_HASH);

                                    fiona_send(&mut generator, stream.clone()).await;

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

        let mut h = blake2::Blake2b512::new();
        h.update(&bytes);
        let digest = h.finalize();

        assert_eq!(digest.as_slice(), EXPECTED_HASH);
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
