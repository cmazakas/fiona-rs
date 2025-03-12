use std::{
    hash::{DefaultHasher, Hasher},
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

use rand::SeedableRng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

extern crate rand;
extern crate tokio;

mod utils;

const NR_FILES: u32 = 512;
const BUF_SIZE: usize = 256 * 1024;

static START_FLAG: AtomicBool = AtomicBool::new(false);

fn make_bytes() -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1234);
    let mut bytes = vec![0_u8; BUF_SIZE];
    rand::RngCore::fill_bytes(&mut rng, &mut bytes);
    bytes
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
            while !START_FLAG.load(std::sync::atomic::Ordering::Relaxed) {
                std::hint::spin_loop();
            }

            let start = Instant::now();
            let mut join_set = tokio::task::JoinSet::new();
            for _ in 0..NR_FILES {
                join_set.spawn(async {
                    let addr = "127.0.0.1:8080".parse().unwrap();

                    let socket = tokio::net::TcpSocket::new_v4().unwrap();
                    let mut client = socket.connect(addr).await.unwrap();

                    let mut buf = [0; 4096];

                    let bytes = make_bytes();
                    let bytes = bytes.chunks_exact(16 * 1024);

                    let mut total_received = 0;
                    let mut h = DefaultHasher::new();

                    let mut send_buf = Vec::<u8>::with_capacity(16 * 1024);

                    for chunk in bytes {
                        send_buf.extend_from_slice(chunk);
                        let n =
                            tokio::time::timeout(Duration::from_secs(3), client.write(&send_buf))
                                .await
                                .unwrap()
                                .unwrap();

                        assert_eq!(n, 16 * 1024);
                        send_buf.clear();
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
                });
            }
            join_set.join_all().await;
            println!("tokio client loop took: {:?}", start.elapsed());
        });
    });

    rt.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
            .await
            .unwrap();

        START_FLAG.store(true, std::sync::atomic::Ordering::Relaxed);

        let mut join_set = tokio::task::JoinSet::new();
        let start = Instant::now();

        for _ in 0..NR_FILES {
            let mut stream = listener.accept().await.unwrap().0;

            join_set.spawn(async move {
                let mut buf = [0; 4096];

                let bytes = make_bytes();
                let bytes = bytes.chunks_exact(16 * 1024);

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
                }

                let digest = h.finish();
                assert_eq!(digest, 5326650159322985034);

                for chunk in bytes {
                    send_buf.extend_from_slice(chunk);
                    let n = tokio::time::timeout(Duration::from_secs(3), stream.write(&send_buf))
                        .await
                        .unwrap()
                        .unwrap();

                    assert_eq!(n, 16 * 1024);
                    send_buf.clear();
                }
            });
        }
        join_set.join_all().await;

        println!("tokio accept loop took: {:?}", start.elapsed());
    });

    t.join().unwrap();

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

    utils::run_once("tokio echo2", tokio_echo).unwrap();
}
