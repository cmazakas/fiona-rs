extern crate rand;

use std::{net::Ipv4Addr, sync::atomic::AtomicU64, time::Instant};

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

fn fiona_echo() {
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
}

fn main() {
    let prev = Instant::now();

    fiona_echo();
    let dur = Instant::now().duration_since(prev);
    assert_eq!(
        NUM_RUNS.load(std::sync::atomic::Ordering::Relaxed),
        (2 * NR_FILES).into()
    );
    println!("duration: {dur:?}");
}
