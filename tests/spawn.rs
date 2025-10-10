// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#![feature(local_waker)]

use std::{
    cell::{Cell, RefCell},
    future::{Future, poll_fn},
    hash::{DefaultHasher, Hasher},
    pin::Pin,
    rc::Rc,
    sync::{Arc, Mutex},
    task::{LocalWaker, Poll, Waker},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use futures::{SinkExt, StreamExt, stream::FuturesUnordered};
use rand::SeedableRng;

struct YieldFuture<T: Unpin>
{
    yielded: bool,
    done: bool,
    t: Option<T>,
}

impl<T: Unpin> Future for YieldFuture<T>
{
    type Output = T;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
            -> std::task::Poll<Self::Output>
    {
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

impl<T: Unpin> YieldFuture<T>
{
    fn new(t: T) -> Self
    {
        Self { yielded: false,
               done: false,
               t: Some(t) }
    }
}

fn yield_now<T: Unpin>(t: T) -> YieldFuture<T>
{
    YieldFuture::new(t)
}

//-----------------------------------------------------------------------------

struct WakerFuture;

impl Future for WakerFuture
{
    type Output = Waker;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
    {
        Poll::Ready(cx.waker().clone())
    }
}

//-----------------------------------------------------------------------------

struct TimerFuture<T>
{
    dur: Duration,
    t: Option<JoinHandle<()>>,
    done: bool,
    value: Option<T>,
}

impl<T> TimerFuture<T>
{
    fn new(dur: Duration, val: T) -> Self
    {
        Self { dur,
               t: None,
               done: false,
               value: Some(val) }
    }
}

impl<T: Unpin> Future for TimerFuture<T>
{
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
    {
        assert!(!self.done);
        match self.t.take() {
            None => {
                let dur = self.dur;
                let waker = cx.waker().clone();
                self.t = Some(std::thread::spawn(move || {
                                  std::thread::sleep(dur);
                                  waker.wake();
                              }));
                Poll::Pending
            }
            Some(join_handle) => {
                join_handle.join().unwrap();
                self.done = true;
                Poll::Ready(self.value.take().unwrap())
            }
        }
    }
}

fn wait_for<T>(dur: Duration, t: T) -> TimerFuture<T>
{
    TimerFuture::new(dur, t)
}

//-----------------------------------------------------------------------------

#[test]
fn await_simple()
{
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
fn await_value()
{
    async fn make_vec() -> Vec<i32>
    {
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
fn await_forgotten()
{
    async fn make_vec() -> Vec<i32>
    {
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
fn await_forgotten_recursive()
{
    static mut P_FUTURE: *mut fiona::SpawnFuture<Vec<i32>> = std::ptr::null_mut();

    async fn make_vec() -> Vec<i32>
    {
        assert!(unsafe { !P_FUTURE.is_null() });
        let f = unsafe { Pin::new_unchecked(&mut *P_FUTURE) };

        let w = WakerFuture.await;
        let mut cx = std::task::Context::from_waker(&w);

        // In theory, turning this into a .await point should either infinitely loop or
        // just hang indefinitely. A simple, single poll() call should hopefully
        // be enough to help us detect soundness issues with a future poll()'ing
        // its own associated SpawnFuture.
        assert!(f.poll(&mut cx).is_pending());
        vec![1, 2, 3, 4]
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    let c = Rc::new(RefCell::new(0));
    let c2 = c.clone();

    async fn f(ex: fiona::Executor, c2: Rc<RefCell<i32>>)
    {
        let h = ex.spawn(make_vec());
        *c2.borrow_mut() = 4321;
        unsafe {
            P_FUTURE = Box::into_raw(Box::new(h));
        }
    }
    ex.spawn(f(ex.clone(), c2));

    assert_eq!(ioc.run(), 2);
    assert_eq!(*c.borrow(), 4321);

    assert!(unsafe { !P_FUTURE.is_null() });
    let f = unsafe { Box::from_raw(P_FUTURE) };
    let f = std::pin::pin!(f);

    let w = Waker::noop();
    let mut cx = std::task::Context::from_waker(w);
    if let Poll::Ready(v) = f.poll(&mut cx) {
        assert_eq!(v, vec![1, 2, 3, 4]);
    } else {
        unreachable!();
    };
}

#[test]
fn await_sequential()
{
    async fn identity(x: i32) -> i32
    {
        yield_now(x).await
    }

    async fn sequential(ex: fiona::Executor)
    {
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
fn await_sequential_rev()
{
    async fn identity(x: i32) -> i32
    {
        x
    }

    async fn sequential(ex: fiona::Executor)
    {
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
fn await_non_send()
{
    struct ThreadLocal
    {
        x: Rc<Cell<i32>>,
    }

    async fn make_int() -> i32
    {
        1234
    }

    async fn child(ex: fiona::Executor) -> ThreadLocal
    {
        let x = ex.spawn(make_int()).await;
        ThreadLocal { x: Rc::new(Cell::new(x)) }
    }

    async fn parent(ex: fiona::Executor)
    {
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
fn await_future_relocated()
{
    async fn task() -> i32
    {
        1234
    }

    async fn child(f: fiona::SpawnFuture<i32>)
    {
        let x = f.await;
        yield_now(()).await;
        assert_eq!(x, 1234);
    }

    async fn parent(ex: fiona::Executor)
    {
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
fn await_future_panics()
{
    async fn task(x: i32)
    {
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

#[test]
fn await_from_main()
{
    // TODO: kind of an abstraction leak that this is possible
    // but I don't see a good solution here

    struct PanicWaker;
    impl std::task::Wake for PanicWaker
    {
        fn wake(self: std::sync::Arc<Self>)
        {
            panic!();
        }
    }

    let mut ioc = fiona::IoContext::new();
    let mut future = Box::pin(ioc.get_executor().spawn(async { 1234 }));
    let mut future2 = Box::pin(ioc.get_executor().spawn(async { vec![1234] }));
    ioc.run();

    // Make sure this remains to test SpawnFuture outliving its backing IoContext.
    drop(ioc);

    let w = Waker::from(Arc::new(PanicWaker));
    let mut cx = std::task::Context::from_waker(&w);
    match future.as_mut().poll(&mut cx) {
        Poll::Pending => panic!(),
        Poll::Ready(x) => assert_eq!(x, 1234),
    }
    match future2.as_mut().poll(&mut cx) {
        Poll::Pending => panic!(),
        Poll::Ready(x) => assert_eq!(x, vec![1234]),
    }
}

#[test]
fn await_stress_test()
{
    async fn task(x: i32)
    {
        let x2 = yield_now(x).await;
        assert_eq!(x2, x);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    for i in 0..100_000 {
        ex.spawn(task(i));
    }

    let n = ioc.run();
    assert_eq!(n, 100_000);
}

#[test]
fn await_cycle()
{
    struct RecursiveFuture
    {
        this: Rc<RefCell<Option<fiona::SpawnFuture<()>>>>,
        recursions: i32,
        done: bool,
    }

    impl Future for RecursiveFuture
    {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
        {
            assert!(!self.done);

            let r;
            {
                let mut tmp = self.this.borrow_mut();
                let future = tmp.as_mut();
                r = unsafe { Pin::new_unchecked(&mut future.unwrap()).poll(cx) };
            }

            if self.recursions > 100 {
                self.done = true;
                *self.this.borrow_mut() = None;
                return Poll::Ready(());
            }

            match r {
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    self.recursions += 1;
                    Poll::Pending
                }
                Poll::Ready(()) => panic!(),
            }
        }
    }

    async fn start(ex: fiona::Executor)
    {
        let p = Rc::new(RefCell::new(None));
        let f = ex.spawn(RecursiveFuture { this: p.clone(),
                                           recursions: 0,
                                           done: false });
        *p.borrow_mut() = Some(f);
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.spawn(start(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 2);
}

#[test]
fn await_timer()
{
    let dur = Duration::from_millis(250);

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.spawn(async move {
          let v = wait_for(dur, vec![1, 2, 3, 4]).await;
          assert_eq!(v, vec![1, 2, 3, 4]);
      });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn await_manually_polled()
{
    async fn f1(ex: fiona::Executor)
    {
        let w = WakerFuture.await;

        // test the case where we manually poll a sibling task, attaching ourselves as
        // the continuation our task then immediately finishes
        //
        let f = std::pin::pin!(ex.spawn(f2()));

        let mut cx = std::task::Context::from_waker(&w);
        let r = f.poll(&mut cx);
        assert!(r.is_pending());
    }

    async fn f2() -> Box<i32>
    {
        yield_now(Box::new(1234)).await
    }

    async fn f3()
    {
        let x = yield_now(4321).await;
        let p = yield_now(Box::new(x)).await;
        assert_eq!(*p, 4321);
    }

    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();
    ex.spawn(f1(ex.clone()));
    ex.spawn(f3());

    let n = ioc.run();
    assert_eq!(n, 3);
}

#[test]
fn await_manually_polled_early_drop()
{
    // want to attempt to test the property that the main task we're waiting on goes
    // out of scope when an external thread completes and tries to use the Waker

    static mut NUM_RUNS: u64 = 0;

    async fn f1()
    {
        let w = WakerFuture.await;
        let mut cx = std::task::Context::from_waker(&w);

        let mut f = wait_for(Duration::from_millis(250), ());
        assert!(std::pin::pin!(&mut f).poll(&mut cx).is_pending());
        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();
    ex.spawn(f1());

    let n = ioc.run();

    std::thread::sleep(Duration::from_millis(550));

    assert_eq!(n, 1);
    assert_eq!(unsafe { NUM_RUNS }, n);
}

#[test]
fn await_manual_timeslice()
{
    // want to test that a user can appropriately use our Waker to time-slice
    // long-standing operations, letting other things in the run queue process

    static mut NUM_RUNS: u64 = 0;

    let vec = Rc::new(RefCell::new(Vec::<i32>::new()));

    let flag = Rc::new(RefCell::new(false));

    async fn f1(flag: Rc<RefCell<bool>>, vec: Rc<RefCell<Vec<i32>>>)
    {
        let mut v1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        poll_fn(|cx| {
            assert!(!*flag.borrow());
            *flag.borrow_mut() = true;

            vec.borrow_mut().extend(v1.drain(0..2));
            if v1.is_empty() {
                return Poll::Ready(());
            }

            cx.waker().wake_by_ref();
            Poll::Pending
        }).await;

        unsafe { NUM_RUNS += 1 };
    }

    async fn f2(flag: Rc<RefCell<bool>>, vec: Rc<RefCell<Vec<i32>>>)
    {
        let mut v2 = vec![10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

        poll_fn(|cx| {
            assert!(*flag.borrow());
            *flag.borrow_mut() = false;

            vec.borrow_mut().extend(v2.drain(0..2));
            if v2.is_empty() {
                return Poll::Ready(());
            }

            cx.waker().wake_by_ref();
            Poll::Pending
        }).await;

        unsafe { NUM_RUNS += 1 };
    }

    let mut ioc = fiona::IoContext::new();

    let ex = ioc.get_executor();
    ex.spawn(f1(flag.clone(), vec.clone()));
    ex.spawn(f2(flag.clone(), vec.clone()));

    let n = ioc.run();

    assert_eq!(n, 2);
    assert_eq!(unsafe { NUM_RUNS }, n);

    assert_eq!(*vec.borrow(), vec![1, 2, 10, 9, 3, 4, 8, 7, 5, 6, 6, 5, 7, 8, 4, 3, 9, 10, 2, 1]);
}

//-----------------------------------------------------------------------------

#[test]
fn await_futures_ordered()
{
    async fn identity(x: i32) -> i32
    {
        let mut v = vec![x];
        v = yield_now(v).await;
        v[0]
    }

    async fn sequential(ex: fiona::Executor)
    {
        let mut futures = Vec::<fiona::SpawnFuture<i32>>::new();
        for i in 0..10 {
            futures.push(ex.spawn(identity(i + 1)));
        }

        let mut ordered: futures::stream::FuturesOrdered<fiona::SpawnFuture<i32>> =
            futures.into_iter().collect();

        let mut i = 1;
        while let Some(x) = ordered.next().await {
            assert_eq!(x, i);
            i += 1;
        }
    }

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn(sequential(ex.clone()));

    assert_eq!(ioc.run(), 11);
}

#[test]
fn await_futures_unordered()
{
    async fn launch_timers(ex: fiona::Executor)
    {
        let f1 = ex.spawn(wait_for(Duration::from_millis(300), 3));
        let f2 = ex.spawn(wait_for(Duration::from_millis(400), 4));
        let f3 = ex.spawn(wait_for(Duration::from_millis(100), 1));
        let f4 = ex.spawn(wait_for(Duration::from_millis(200), 2));

        let futures = [f1, f2, f3, f4];
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

    ex.spawn(launch_timers(ex.clone()));

    let n = ioc.run();
    assert_eq!(n, 5);
}

#[test]
fn await_local_waker_outlives()
{
    let p = Rc::<RefCell<Option<LocalWaker>>>::new(RefCell::new(None));
    let q = Rc::<RefCell<Option<Waker>>>::new(RefCell::new(None));

    {
        let mut ioc = fiona::IoContext::new();
        let ex = ioc.get_executor();

        {
            let p = p.clone();
            let q = q.clone();
            ex.spawn(async move {
                  poll_fn(|cx| {
                      *p.borrow_mut() = Some(cx.local_waker().clone());
                      *q.borrow_mut() = Some(cx.waker().clone());
                      Poll::Ready(())
                  }).await;
              });
        }

        ioc.run();
    }

    let local_waker = (*p).take().unwrap();
    local_waker.wake_by_ref();
    local_waker.wake();

    let waker = (*q).take().unwrap();
    waker.wake_by_ref();
}

#[test]
fn await_futures_mpsc()
{
    let mut ioc = fiona::IoContext::new();

    async fn receiver(mut rx: futures::channel::mpsc::Receiver<i32>)
    {
        let before = Instant::now();

        let value = rx.next().await.unwrap();

        let elapsed = Instant::now() - before;
        assert!(elapsed >= Duration::from_millis(100));

        assert_eq!(value, 1234);
    }

    async fn sender(_ex: fiona::Executor, mut tx: futures::channel::mpsc::Sender<i32>)
    {
        std::thread::sleep(Duration::from_millis(100));
        tx.send(1234).await.unwrap();
    }

    let (tx, rx) = futures::channel::mpsc::channel::<i32>(1024);

    let ex = ioc.get_executor();
    ex.spawn(receiver(rx));

    let thread = std::thread::spawn(move || {
        let mut ioc = fiona::IoContext::new();
        let ex = ioc.get_executor();

        ex.clone().spawn(sender(ex.clone(), tx));

        let n = ioc.run();
        assert_eq!(n, 1);
    });

    let n = ioc.run();
    assert_eq!(n, 1);

    thread.join().unwrap();
}

#[test]
fn await_rayon_tasks()
{
    // Want to test that our IoContext can toss work onto a Rayon threadpool which
    // we then await on in bulk.

    fn make_bytes() -> Vec<u8>
    {
        let mut rng = rand::rngs::StdRng::seed_from_u64(1234);
        let mut bytes = vec![0_u8; 16 * 1024 * 1024];
        rand::RngCore::fill_bytes(&mut rng, &mut bytes);
        bytes
    }

    struct RayonFuture
    {
        initiated: bool,
        done: bool,
        thread_pool: Rc<rayon::ThreadPool>,
        value: Arc<Mutex<Option<u64>>>,
    }

    impl RayonFuture
    {
        fn new(thread_pool: Rc<rayon::ThreadPool>) -> RayonFuture
        {
            RayonFuture { initiated: false,
                          done: false,
                          thread_pool,
                          value: Arc::new(Mutex::new(None)) }
        }
    }

    impl Future for RayonFuture
    {
        type Output = u64;

        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output>
        {
            assert!(!self.done);

            if self.initiated {
                let mut guard = self.value.lock().unwrap();
                if guard.is_none() {
                    return Poll::Pending;
                }

                let value = guard.take().unwrap();
                drop(guard);

                self.done = true;
                return Poll::Ready(value);
            }

            let value = self.value.clone();
            let waker = cx.waker().clone();
            self //
                .thread_pool
                .spawn(move || {
                    let bytes = make_bytes();

                    let mut h = DefaultHasher::new();

                    h.write(&bytes);

                    *value.lock().unwrap() = Some(h.finish());
                    waker.wake();
                });

            self.initiated = true;

            Poll::Pending
        }
    }

    async fn fiona_task(thread_pool: Rc<rayon::ThreadPool>)
    {
        let mut tasks = FuturesUnordered::new();
        for _ in 0..64 {
            tasks.push(RayonFuture::new(thread_pool.clone()));
        }

        while let Some(h) = tasks.next().await {
            assert_eq!(h, 17722614205829968049);
        }
    }

    let thread_pool = Rc::new(rayon::ThreadPoolBuilder::new().num_threads(4)
                                                             .build()
                                                             .unwrap());

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();
    ex.spawn(fiona_task(thread_pool));
    let _n = ioc.run();
}
