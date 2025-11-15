extern crate futures;

use std::{
    hash::DefaultHasher,
    net::Ipv4Addr,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use fiona::tcp::AcceptorOpts;
use nix::{errno::Errno, libc::ENOBUFS};
use rand::SeedableRng;

static TOTAL_CONNS: AtomicU64 = AtomicU64::new(0);

const CQ_ENTRIES: u32 = 64 * 1024;
const BUF_SIZE: usize = 256 * 1024;
const RECV_BUF_SIZE: usize = 8 * 1024;
const NUM_BUFS: u32 = 32 * 1024;
const CLIENT_BGID: u16 = 72;
const SERVER_BGID: u16 = 27;

use std::hash::Hasher;

struct ByteGen
{
    rng: rand::rngs::StdRng,
}

impl ByteGen
{
    fn new() -> Self
    {
        Self { rng: rand::rngs::StdRng::seed_from_u64(1234) }
    }

    fn write(&mut self, bytes: &mut [u8; 16 * 1024])
    {
        rand::RngCore::fill_bytes(&mut self.rng, bytes);
    }
}

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

fn fiona_echo_client(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    ex.register_buf_group(CLIENT_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    ex //
      .clone()
      .spawn(async move {
          let mut tasks = Vec::new();
          for _ in 0..nr_files {
              tasks.push(ex.spawn(fiona_echo_client_impl(ex.clone(), ipv4_addr, port)));
          }

          for h in tasks {
              h.await;
          }
      });

    ioc.run();

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

        if TOTAL_CONNS.load(Ordering::Relaxed) < 10 {
            panic!()
        }
    }
}

fn fiona_echo_server(ipv4_addr: Ipv4Addr, port: u16, nr_files: u32) -> Result<(), String>
{
    TOTAL_CONNS.store(nr_files as u64, Ordering::Relaxed);

    let mut ioc = make_io_context(nr_files);
    let ex = ioc.get_executor();

    ex.register_buf_group(SERVER_BGID, NUM_BUFS, RECV_BUF_SIZE)
      .unwrap();

    let opts = &AcceptorOpts { reuse_addr: true,
                               reuse_port: true };
    let acceptor =
        fiona::tcp::Acceptor::bind_ipv4_with_params(ex.clone(), ipv4_addr, port, opts).unwrap();

    ex.clone().spawn(async move {
                  loop {
                      match acceptor.accept().await {
                          Err(Errno::ECANCELED) => break,
                          Err(e) => panic!("{e:?}"),
                          Ok(stream) => {
                              stream.get_executor().spawn(fiona_echo_server_impl(stream));
                          }
                      }

                      if TOTAL_CONNS.fetch_sub(1, Ordering::Relaxed) == 1 {
                          break;
                      }
                  }
              });

    ioc.run();

    Ok(())
}

#[test]
fn tcp_stress_panicking()
{
    let port = 8000;
    let nr_files = 100;

    let t1 = std::thread::spawn(move || fiona_echo_server(Ipv4Addr::LOCALHOST, port, nr_files));
    std::thread::sleep(Duration::from_millis(100));
    let t2 = std::thread::spawn(move || fiona_echo_client(Ipv4Addr::LOCALHOST, port, nr_files));

    t1.join().unwrap_err();
    t2.join().unwrap_err();
}
