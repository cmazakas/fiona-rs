use std::{net::Ipv4Addr, time::Duration};

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
        let acceptor = fiona::tcp::Acceptor::bind_ipv4(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 0;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        let ex2 = ex.clone();
        let client_task = ex.clone() //
                            .spawn(async move {
                                let ex = ex2;
                                let client = fiona::tcp::Client::new(ex.clone());

                                client.connect_ipv4(Ipv4Addr::LOCALHOST, port)
                                      .await
                                      .unwrap();

                                client
                            });

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);
        stream.set_timeout(Duration::from_millis(500));

        let client = client_task.await;
        client.set_buf_group(bgid);

        let client2 = client.clone();
        ex.spawn(async move {
              let client = client2;
              let msg = String::from("hello, world!").into_bytes();
              let (num_sent, buf) = client.send(msg).await;
              assert_eq!(num_sent.unwrap(), buf.len());
          })
          .await;

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        let client2 = client.clone();
        ex.spawn(async move {
              let client = client2;
              let msg = String::from("hello, world!").into_bytes();
              let (num_sent, buf) = client.send(msg).await;
              assert_eq!(num_sent.unwrap(), buf.len());
          })
          .await;

        let bufs = stream.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "hello, world!");

        let err = stream.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        unsafe { NUM_RUNS += 1 };
    }

    async fn client_timeout(ex: fiona::Executor)
    {
        let acceptor = fiona::tcp::Acceptor::bind_ipv4(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
        let port = acceptor.port();

        let bgid = 1;
        let num_bufs = 64;
        let buf_len = 8;

        ex.register_buf_group(bgid, num_bufs, buf_len).unwrap();

        let ex2 = ex.clone();
        let client_task = ex.clone() //
                            .spawn(async move {
                                let ex = ex2;
                                let client = fiona::tcp::Client::new(ex.clone());

                                client.connect_ipv4(Ipv4Addr::LOCALHOST, port)
                                      .await
                                      .unwrap();

                                client
                            });

        let stream = acceptor.accept().await.unwrap();
        stream.set_buf_group(bgid);

        let client = client_task.await;
        client.set_buf_group(bgid);
        client.set_timeout(Duration::from_millis(500));

        let mut msg = String::from("wonderful day").into_bytes();
        let (num_sent, buf) = stream.send(msg).await;
        assert_eq!(num_sent.unwrap(), buf.len());
        msg = buf;

        let bufs = client.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "wonderful day");

        let err = client.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        let (num_sent, buf) = stream.send(msg).await;
        assert_eq!(num_sent.unwrap(), buf.len());

        let bufs = client.recv().await.unwrap();
        assert_eq!(bufs_to_string(&bufs), "wonderful day");

        let err = client.recv().await.unwrap_err();
        assert_eq!(err, Errno::ECANCELED);

        unsafe { NUM_RUNS += 1 };
    }

    static mut NUM_RUNS: u64 = 0;

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.clone().spawn(server_timeout(ex.clone()));
    ex.clone().spawn(client_timeout(ex.clone()));

    let _n = ioc.run();
    assert_eq!(unsafe { NUM_RUNS }, 2);
}
