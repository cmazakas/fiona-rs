extern crate rand;
extern crate tokio;

mod utils;

use std::{net::Ipv4Addr, sync::atomic::AtomicU64, time::Duration};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

const NR_FILES: u32 = 5000;
const NUM_MSGS: u32 = 1000;

static NUM_RUNS: AtomicU64 = AtomicU64::new(0);

fn fiona_echo() -> Result<(), String>
{
    fn make_io_context() -> fiona::IoContext
    {
        let params = &fiona::IoContextParams { sq_entries: 256,
                                               cq_entries: CQ_ENTRIES,
                                               nr_files: 4 * NR_FILES };

        fiona::IoContext::with_params(params)
    }

    const CQ_ENTRIES: u32 = 64 * 1024;

    const SERVER_BGID: u16 = 27;
    const CLIENT_BGID: u16 = 72;

    let mut ioc = make_io_context();
    let ex = ioc.get_executor();

    let acceptor =
        fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::new(192, 168, 1, 79), 0).unwrap();
    let port = acceptor.port();

    ex.register_buf_group(SERVER_BGID, 16 * 1024, 1024).unwrap();

    ex.clone().spawn(async move {
                  for _ in 0..NR_FILES {
                      let stream = acceptor.accept().await.unwrap();
                      ex.clone().spawn(async move {
                                    stream.set_buf_group(SERVER_BGID);

                                    let mut message = Vec::with_capacity(13);

                                    for _ in 0..NUM_MSGS {
                                        let bufs = stream.recv().await.unwrap();
                                        let buf = bufs.iter().next().unwrap();
                                        assert_eq!(buf, "hello, world!".as_bytes());

                                        message.extend_from_slice(buf);
                                        message = stream.send(message).await.unwrap();
                                        assert!(message.is_empty());
                                    }

                                    NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                });
                  }
              });

    let t = std::thread::spawn(move || {
        let mut ioc = make_io_context();
        let ex = ioc.get_executor();
        ex.register_buf_group(CLIENT_BGID, 16 * 1024, 1024).unwrap();

        for _ in 0..NR_FILES {
            let ex2 = ex.clone();
            ex.clone().spawn(async move {
                          let client = fiona::tcp::Client::new(ex2);
                          client.connect_ipv4(Ipv4Addr::new(192, 168, 1, 79), port)
                                .await
                                .unwrap();

                          client.set_buf_group(CLIENT_BGID);
                          let mut message = Vec::with_capacity(13);

                          for _ in 0..NUM_MSGS {
                              message.extend_from_slice("hello, world!".as_bytes());
                              message = client.send(message).await.unwrap();
                              assert!(message.is_empty());

                              let bufs = client.recv().await.unwrap();
                              let buf = bufs.iter().next().unwrap();
                              assert_eq!(buf, "hello, world!".as_bytes());
                          }

                          NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                      });
        }

        let _n = ioc.run();
    });

    let _n = ioc.run();
    t.join().unwrap();

    assert_eq!(NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed), (2 * NR_FILES).into());

    Ok(())
}

fn tokio_echo() -> Result<(), String>
{
    let rt = tokio::runtime::Builder::new_current_thread().enable_all()
                                                          .build()
                                                          .unwrap();

    let t = std::thread::spawn(|| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all()
                                                              .build()
                                                              .unwrap();

        rt.block_on(async {
              let mut join_set = tokio::task::JoinSet::new();
              for _ in 0..NR_FILES {
                  join_set.spawn(async {
                              let addr = "192.168.1.79:8080".parse().unwrap();

                              let socket = tokio::net::TcpSocket::new_v4().unwrap();
                              let mut client = socket.connect(addr).await.unwrap();

                              let message = String::from("hello, world!").into_bytes();
                              for _ in 0..NUM_MSGS {
                                  let n = tokio::time::timeout(Duration::from_secs(3),
                                                               client.write(&message)).await
                                                                                      .unwrap()
                                                                                      .unwrap();

                                  assert_eq!(n, 13);

                                  let mut buf = [0; 1024];
                                  let n = tokio::time::timeout(Duration::from_secs(3),
                                                               client.read(&mut buf)).await
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
          let listener = tokio::net::TcpListener::bind("192.168.1.79:8080").await
                                                                           .unwrap();

          let mut join_set = tokio::task::JoinSet::new();
          for _ in 0..NR_FILES {
              let mut stream = listener.accept().await.unwrap().0;

              join_set.spawn(async move {
                          for _ in 0..NUM_MSGS {
                              let mut buf = [0; 1024];
                              let n = tokio::time::timeout(Duration::from_secs(3),
                                                           stream.read(&mut buf)).await
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

    assert_eq!(NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed), (2 * NR_FILES).into());

    Ok(())
}

fn main()
{
    utils::run_once("fiona_echo", fiona_echo).unwrap();
    utils::run_once("tokio_echo", tokio_echo).unwrap();
}
