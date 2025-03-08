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

    const SERVER_BGID: u16 = 27;

    let mut ioc = make_io_context();
    let ex = ioc.get_executor();

    let acceptor =
        fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::new(192, 168, 1, 79), 8081).unwrap();

    ex.register_buf_group(SERVER_BGID, 32 * 1024, 1024).unwrap();

    static mut SERVER_DUR: Duration = Duration::new(0, 0);

    ex.clone().spawn(async move {
        for _ in 0..NR_FILES {
            let stream = acceptor.accept().await.unwrap();
            ex.clone().spawn(async move {
                let start = Instant::now();

                stream.set_buf_group(SERVER_BGID);

                let bytes = make_bytes();
                let mut bytes = bytes.chunks_exact(16 * 1024);

                let mut total_received = 0;
                let mut h = DefaultHasher::new();

                let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                // let mut recv_dur = Duration::new(0, 0);
                // let mut num_recvs = 0;

                while total_received < BUF_SIZE {
                    // let recv_start = Instant::now();

                    let bufs = stream.recv().await.unwrap();

                    // num_recvs += 1;
                    // recv_dur += Instant::now() - recv_start;

                    for buf in &bufs {
                        h.write(buf);
                        total_received += buf.len();
                    }

                    if let Some(chunk) = bytes.next() {
                        send_buf.extend_from_slice(chunk);
                        send_buf = stream.send(send_buf).await.unwrap();
                        assert!(send_buf.is_empty());
                    }
                }

                for chunk in bytes {
                    send_buf.extend_from_slice(chunk);
                    send_buf = stream.send(send_buf).await.unwrap();
                    assert!(send_buf.is_empty());
                }

                let digest = h.finish();
                assert_eq!(digest, 5326650159322985034);

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let end = Instant::now();
                let dur = end - start;
                unsafe { SERVER_DUR += dur };

                // println!(
                //     "average recv time: {:?}, number of recvs: {}",
                //     recv_dur / num_recvs,
                //     num_recvs
                // );
                // println!("fiona server session took: {dur:?}");
            });
        }
    });

    let _n = ioc.run();

    assert_eq!(
        NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed),
        NR_FILES.into()
    );

    println!("fiona average server session duration: {:?}", unsafe {
        SERVER_DUR / NR_FILES
    });

    Ok(())
}

fn tokio_echo() -> Result<(), String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    static mut SERVER_DUR: Duration = Duration::new(0, 0);

    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("192.168.1.79:8080")
            .await
            .unwrap();

        let mut join_set = tokio::task::JoinSet::new();
        for _ in 0..NR_FILES {
            let mut stream = listener.accept().await.unwrap().0;

            join_set.spawn(async move {
                let start = Instant::now();

                let mut buf = [0; 1024];

                let bytes = make_bytes();
                let mut bytes = bytes.chunks_exact(16 * 1024);

                let mut total_received = 0;
                let mut h = DefaultHasher::new();

                let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                while total_received < BUF_SIZE {
                    let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buf))
                        .await
                        .unwrap()
                        .unwrap();

                    h.write(&buf[0..n]);
                    total_received += n;

                    if let Some(chunk) = bytes.next() {
                        send_buf.extend_from_slice(chunk);
                        let n =
                            tokio::time::timeout(Duration::from_secs(3), stream.write(&send_buf))
                                .await
                                .unwrap()
                                .unwrap();

                        assert_eq!(n, 16 * 1024);
                        send_buf.clear();
                    }
                }

                for chunk in bytes {
                    send_buf.extend_from_slice(chunk);
                    let n = tokio::time::timeout(Duration::from_secs(3), stream.write(&send_buf))
                        .await
                        .unwrap()
                        .unwrap();

                    assert_eq!(n, 16 * 1024);
                    send_buf.clear();
                }

                let digest = h.finish();
                assert_eq!(digest, 5326650159322985034);

                NUM_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                let end = Instant::now();
                let dur = end - start;
                unsafe { SERVER_DUR += dur };
                // println!("tokio server session took: {dur:?}");
            });
        }
        join_set.join_all().await;
    });

    assert_eq!(
        NUM_RUNS.swap(0, std::sync::atomic::Ordering::Relaxed),
        NR_FILES.into()
    );

    println!("tokio average server session duration: {:?}", unsafe {
        SERVER_DUR / NR_FILES
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
