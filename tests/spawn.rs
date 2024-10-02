use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    task::Poll,
};

use futures::StreamExt;

struct YieldFuture<T: Unpin> {
    yielded: bool,
    done: bool,
    t: Option<T>,
}

impl<T: Unpin> Future for YieldFuture<T> {
    type Output = T;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.done);
        match self.yielded {
            false => {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            true => {
                self.done = true;
                Poll::Ready(self.t.take().unwrap())
            }
        }
    }
}

impl<T: Unpin> YieldFuture<T> {
    fn new(t: T) -> Self {
        Self {
            yielded: false,
            done: false,
            t: Some(t),
        }
    }
}

fn yield_now<T: Unpin>(t: T) -> YieldFuture<T> {
    YieldFuture::new(t)
}

//-----------------------------------------------------------------------------

#[test]
fn await_simple() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let c = Rc::new(RefCell::new(0));
    {
        let c2 = c.clone();
        ex.spawn(async move {
            *c2.borrow_mut() = 1234;
        });
    }
    assert_eq!(ioc.run(), 1);
    assert_eq!(*c.borrow(), 1234);
}

#[test]
fn await_value() {
    async fn make_vec() -> Vec<i32> {
        yield_now(()).await;
        vec![1, 2, 3, 4]
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let c = Rc::new(RefCell::new(0));
    {
        let c2 = c.clone();
        let ex2 = ex.clone();
        ex.spawn(async move {
            let v = ex2.spawn(make_vec()).await;
            *c2.borrow_mut() = v.len();
        });
    }
    assert_eq!(ioc.run(), 2);
    assert_eq!(*c.borrow(), 4);
}

#[test]
fn await_forgotten() {
    async fn make_vec() -> Vec<i32> {
        vec![1, 2, 3, 4]
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    static mut P_FUTURE: *mut fiona::SpawnFuture<Vec<i32>> = std::ptr::null_mut();

    let c = Rc::new(RefCell::new(0));
    {
        let c2 = c.clone();
        let ex2 = ex.clone();
        ex.spawn(async move {
            let h = ex2.spawn(make_vec());
            *c2.borrow_mut() = 4321;
            unsafe {
                P_FUTURE = Box::leak(Box::new(h));
            }
        });
    }
    assert_eq!(ioc.run(), 2);
    unsafe {
        drop(Box::from_raw(P_FUTURE));
    }
    assert_eq!(*c.borrow(), 4321);
}

#[test]
fn await_sequential() {
    async fn identity(x: i32) -> i32 {
        x
    }

    async fn sequential(ex: fiona::Executor) {
        let mut futures = Vec::<fiona::SpawnFuture<i32>>::new();
        for i in 0..10 {
            futures.push(ex.spawn(identity(i + 1)));
        }

        let mut values = Vec::<i32>::new();
        for future in futures.into_iter() {
            values.push(future.await);
        }

        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(sequential(ex.clone()));

    assert_eq!(ioc.run(), 11);
}

#[test]
fn await_sequential_rev() {
    async fn identity(x: i32) -> i32 {
        x
    }

    async fn sequential(ex: fiona::Executor) {
        let mut futures = Vec::<fiona::SpawnFuture<i32>>::new();
        for i in 0..10 {
            futures.push(ex.spawn(identity(i + 1)));
        }

        let mut values = Vec::<i32>::new();
        for future in futures.into_iter().rev() {
            values.push(future.await);
        }

        assert_eq!(values, vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(sequential(ex.clone()));

    assert_eq!(ioc.run(), 11);
}

#[test]
fn await_non_send() {
    struct ThreadLocal {
        x: Rc<Cell<i32>>,
    }

    async fn make_int() -> i32 {
        1234
    }

    async fn child(ex: fiona::Executor) -> ThreadLocal {
        let x = ex.spawn(make_int()).await;
        ThreadLocal {
            x: Rc::new(Cell::new(x)),
        }
    }

    async fn parent(ex: fiona::Executor) {
        let f1 = ex.spawn(child(ex.clone()));
        let f2 = ex.spawn(child(ex.clone()));

        let t1 = f1.await;
        let t2 = f2.await;

        assert_eq!(t1.x.get() + t2.x.get(), 2 * 1234);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(parent(ex.clone()));

    assert_eq!(ioc.run(), 5);
}

#[test]
fn await_future_relocated() {
    async fn task() -> i32 {
        1234
    }

    async fn child(f: fiona::SpawnFuture<i32>) {
        let x = f.await;
        yield_now(()).await;
        assert_eq!(x, 1234)
    }

    async fn parent(ex: fiona::Executor) {
        let f = ex.spawn(task());
        ex.spawn(child(f)).await;
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.spawn(parent(ex.clone()));
    let n = ioc.run();

    assert_eq!(n, 3);
}

#[test]
#[should_panic]
fn await_future_panics() {
    async fn task(x: i32) {
        let p = Box::new(x);
        yield_now(()).await;
        assert!(*p < 5);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    for i in 0..10 {
        ex.spawn(task(i));
    }

    ioc.run();
}

//-----------------------------------------------------------------------------

#[test]
fn await_futures_unordered() {
    async fn identity(x: i32) -> i32 {
        let mut v = vec![x];
        v = yield_now(v).await;
        v[0]
    }

    async fn sequential(ex: fiona::Executor) {
        let mut futures = Vec::<fiona::SpawnFuture<i32>>::new();
        for i in 0..10 {
            futures.push(ex.spawn(identity(i + 1)));
        }

        let mut unordered: futures::stream::FuturesUnordered<fiona::SpawnFuture<i32>> =
            futures.into_iter().collect();

        let mut i = 1;
        while let Some(x) = unordered.next().await {
            assert_eq!(x, i);
            i += 1;
        }
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(sequential(ex.clone()));

    assert_eq!(ioc.run(), 11);
}
