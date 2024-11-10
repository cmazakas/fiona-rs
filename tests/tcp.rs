use std::{future::Future, net::Ipv4Addr, task::Poll};

struct WakerFuture;

impl Future for WakerFuture {
    type Output = std::task::Waker;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(cx.waker().clone())
    }
}

#[test]
fn tcp_acceptor_eager_drop() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let mut acceptor = fiona::tcp::Acceptor::new(ex.clone(), Ipv4Addr::LOCALHOST, 0).unwrap();
    assert!(acceptor.port() != 0);

    ex.spawn(async move {
        let mut f = acceptor.accept();

        let w = WakerFuture.await;
        assert!(std::pin::pin!(&mut f)
            .poll(&mut std::task::Context::from_waker(&w))
            .is_pending());

        drop(f);
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}
