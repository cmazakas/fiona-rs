use std::{
    hash::{DefaultHasher, Hasher},
    net::Ipv4Addr,
    sync::atomic::AtomicU64,
    time::{Duration, Instant},
};

use rand::SeedableRng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

extern crate rand;
extern crate tokio;

mod utils;

const NR_FILES: u32 = 512;
const BUF_SIZE: usize = 256 * 1024;

static NUM_RUNS: AtomicU64 = AtomicU64::new(0);

fn make_bytes() -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1234);
    let mut bytes = vec![0_u8; BUF_SIZE];
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
}

fn fiona_echo() -> Result<(), String> {
    fn make_io_context() -> fiona::IoContext {
        let params = &fiona::IoContextParams {
            sq_entries: 256,
            cq_entries: CQ_ENTRIES,
            nr_files: 4 * NR_FILES,
        };

        fiona::IoContext::with_params(params)
    }

    const CQ_ENTRIES: u32 = 64 * 1024;

    const CLIENT_BGID: u16 = 72;

    let mut ioc = make_io_context();
    let ex = ioc.get_executor();
    ex.register_buf_group(CLIENT_BGID, 32 * 1024, 1024).unwrap();

    static mut CLIENT_DUR: Duration = Duration::new(0, 0);

    for _ in 0..NR_FILES {
        let ex2 = ex.clone();
        ex.clone().spawn(async move {
            let start = Instant::now();

            let client = fiona::tcp::Client::new(ex2);
            client
                .connect_ipv4(Ipv4Addr::new(192, 168, 1, 79), 8081)
                .await
                .unwrap();

            client.set_buf_group(CLIENT_BGID);
            let bytes = make_bytes();
            let bytes = bytes.chunks_exact(16 * 1024);

            let mut total_received = 0;
            let mut h = DefaultHasher::new();

            let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

            for chunk in bytes {
                send_buf.extend_from_slice(chunk);
                send_buf = client.send(send_buf).await.unwrap();
                assert!(send_buf.is_empty());

                let bufs = client.recv().await.unwrap();
                for buf in &bufs {
                    h.write(buf);
                    total_received += buf.len();
                }
            }

            while total_received < BUF_SIZE {
                let bufs = client.recv().await.unwrap();
                for buf in &bufs {
                    h.write(buf);
                    total_received += buf.len();
                }
            }

            let digest = h.finish();
            assert_eq!(digest, 5326650159322985034);

            NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let end = Instant::now();
            let dur = end - start;

            unsafe { CLIENT_DUR += dur };
        });
    }

    let _n = ioc.run();

    println!("fiona average client session duration: {:?}", unsafe {
        CLIENT_DUR / NR_FILES
    });

    let _n = ioc.run();

    assert_eq!(
        NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed),
        NR_FILES.into()
    );

    println!("fiona average server session duration: {:?}", unsafe {
        CLIENT_DUR / NR_FILES
    });

    Ok(())
}

fn tokio_echo() -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    static mut CLIENT_DUR: Duration = Duration::new(0, 0);
    rt.block_on(async {
        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..NR_FILES {
            join_set.spawn(async {
                let start = Instant::now();

                let addr = "192.168.1.79:8080".parse().unwrap();

                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                let mut client = socket.connect(addr).await.unwrap();

                let mut buf = [0; 1024];

                let bytes = make_bytes();
                let bytes = bytes.chunks_exact(16 * 1024);

                let mut total_received = 0;
                let mut h = DefaultHasher::new();

                let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                for chunk in bytes {
                    send_buf.extend_from_slice(chunk);
                    let n = tokio::time::timeout(Duration::from_secs(3), client.write(&send_buf))
                        .await
                        .unwrap()
                        .unwrap();

                    assert_eq!(n, 16 * 1024);
                    send_buf.clear();

                    let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();

                    h.write(&buf[0..n]);
                    total_received += n;
                }

                while total_received < BUF_SIZE {
                    let n = tokio::time::timeout(Duration::from_secs(3), client.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();

                    h.write(&buf[0..n]);
                    total_received += n;
                }

                let digest = h.finish();
                assert_eq!(digest, 5326650159322985034);

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let end = Instant::now();
                let dur = end - start;
                unsafe { CLIENT_DUR += dur };
                // println!("tokio client session took: {dur:?}");
            });
        }
        join_set.join_all().await;

        println!("tokio average client session duration: {:?}", unsafe {
            CLIENT_DUR / NR_FILES
        });
    });

    assert_eq!(
        NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed),
        NR_FILES.into()
    );

    println!("tokio average server session duration: {:?}", unsafe {
        CLIENT_DUR / NR_FILES
    });

    Ok(())
}

fn main() {
    {
        let bytes = make_bytes();

        let mut h = DefaultHasher::new();
        h.write(&bytes);
        let digest = h.finish();

        assert_eq!(digest, 5326650159322985034);
    }

    // utils::run_once("fiona echo2", fiona_echo).unwrap();
    utils::run_once("tokio echo2", tokio_echo).unwrap();
    // std::hint::black_box(fiona_echo()).unwrap();
    // std::hint::black_box(tokio_echo()).unwrap();
}
