use std::{net::Ipv4Addr, panic::catch_unwind, time::Duration};

use nix::errno::Errno;

fn bufs_to_string<'a, I>(bufs: I) -> String
    where I: IntoIterator<Item = &'a [u8]>
{
    let mut s = String::new();
    for buf in bufs {
        s.push_str(str::from_utf8(buf).unwrap());
    }
    s
}

#[test]
fn tcp_recv_timeout()
{
    // Test that our recv operation can timeout and then be restarted.

    async fn server_timeout(ex: fiona::Executor)
    {
        let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 0;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        let ex2 = ex.clone();
        let client_fn = async move {
            let ex = ex2;
            let client = fiona::tcp::Client::new(ex.clone());

            client.connect_ipv4(Ipv4Addr::LOCALHOST, port)
                  .await
                  .unwrap();

            client.set_buf_group(bgid);

            let mut msg = String::from("hello, world!").into_bytes();
            let (num_sent, buf) = client.send(msg).await;
            assert_eq!(num_sent.unwrap(), buf.len());
            msg = buf;

            let timer = fiona::time::Timer::new(ex);
            timer.wait(Duration::from_millis(1000)).await.unwrap();

            let (num_sent, buf) = client.send(msg).await;
            assert_eq!(num_sent.unwrap(), buf.len());

            timer.wait(Duration::from_millis(750)).await.unwrap();

            unsafe { NUM_RUNS += 1 };
        };

        ex.clone().spawn(client_fn);

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);
        stream.set_timeout(Duration::from_millis(500));

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        unsafe { NUM_RUNS += 1 };
    }

    async fn client_timeout(ex: fiona::Executor)
    {
        let acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 1;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        let ex2 = ex.clone();
        let client_fn = async move {
            let client = fiona::tcp::Client::new(ex2.clone());

            client.connect_ipv4(Ipv4Addr::LOCALHOST, port)
                  .await
                  .unwrap();

            client.set_buf_group(bgid);
            client.set_timeout(Duration::from_millis(500));

            let bufs = client.recv().await.unwrap();
            assert_eq!(bufs_to_string(&bufs), "wonderful day");

            let err = client.recv().await.unwrap_err();
            assert_eq!(err, Errno::ECANCELED);

            let bufs = client.recv().await.unwrap();
            assert_eq!(bufs_to_string(&bufs), "wonderful day");

            let err = client.recv().await.unwrap_err();
            assert_eq!(err, Errno::ECANCELED);

            unsafe { NUM_RUNS += 1 };
        };

        ex.clone().spawn(client_fn);

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let mut msg = String::from("wonderful day").into_bytes();
        let (num_sent, buf) = stream.send(msg).await;
        assert_eq!(num_sent.unwrap(), buf.len());
        msg = buf;

        let timer = fiona::time::Timer::new(ex);
        timer.wait(Duration::from_millis(750)).await.unwrap();

        let (num_sent, buf) = stream.send(msg).await;
        assert_eq!(num_sent.unwrap(), buf.len());

        timer.wait(Duration::from_millis(750)).await.unwrap();

        unsafe { NUM_RUNS += 1 };
    }

    static mut NUM_RUNS: u64 = 0;

    let r = catch_unwind(|| {
        let mut ioc = fiona::IoContext::new();
        let ex = ioc.get_executor();

        ex.clone().spawn(server_timeout(ex.clone()));
        ex.clone().spawn(client_timeout(ex.clone()));

        let n = ioc.run();
        assert_eq!(n, 4);
        assert_eq!(unsafe { NUM_RUNS }, n);
    });

    if r.is_err() {
        eprintln!("tcp_recv_timeout did not complete successfully but this is not necessarily an error.");
    }
}
