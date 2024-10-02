#![warn(clippy::pedantic)]
#![allow(clippy::mutable_key_type, clippy::missing_panics_doc)]

use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Acquire;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::Ordering::Release;
use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::task::Poll;

struct TaskInner<F: ?Sized + Future<Output = ()> + 'static> {
    strong: AtomicU64,
    weak: AtomicU64,
    future: ManuallyDrop<UnsafeCell<F>>,
}

//-----------------------------------------------------------------------------

struct Task {
    p: NonNull<TaskInner<dyn Future<Output = ()>>>,
    phantom: PhantomData<TaskInner<dyn Future<Output = ()> + 'static>>,
}

impl Task {
    #[must_use]
    pub fn new<F: Future<Output = ()> + 'static>(future: F) -> Self {
        Self {
            p: NonNull::from(Box::leak(Box::new(TaskInner {
                strong: AtomicU64::new(1),
                weak: AtomicU64::new(1),
                future: ManuallyDrop::new(UnsafeCell::new(future)),
            }))),
            phantom: PhantomData,
        }
    }

    fn inner(&self) -> &TaskInner<dyn Future<Output = ()>> {
        unsafe { self.p.as_ref() }
    }

    #[must_use]
    pub fn downgrade(this: &Task) -> Weak {
        this.inner().weak.fetch_add(1, Relaxed);

        Weak {
            p: this.p,
            phantom: PhantomData,
        }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if self.inner().strong.fetch_sub(1, Release) > 1 {
            return;
        }

        // delay the Acquire semantics until we know we need to Drop the T
        self.inner().strong.load(Acquire);

        unsafe {
            ManuallyDrop::drop(&mut (*self.p.as_ptr()).future);
        }

        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }

        self.inner().weak.load(Acquire);

        drop(unsafe { Box::from_raw(self.p.as_ptr()) });
    }
}

impl Clone for Task {
    fn clone(&self) -> Self {
        self.inner().strong.fetch_add(1, Relaxed);
        Self {
            p: self.p,
            phantom: PhantomData,
        }
    }
}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::addr_eq(self.p.as_ptr(), other.p.as_ptr())
    }
}

impl Eq for Task {}

impl Hash for Task {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.p.hash(state);
    }
}

//-----------------------------------------------------------------------------

pub struct Weak {
    p: NonNull<TaskInner<dyn Future<Output = ()>>>,
    phantom: PhantomData<TaskInner<dyn Future<Output = ()>>>,
}

impl Weak {
    fn inner(&self) -> &TaskInner<dyn Future<Output = ()>> {
        unsafe { self.p.as_ref() }
    }

    fn upgrade(&self) -> Option<Task> {
        let mut c = self.inner().strong.load(Relaxed);

        loop {
            if c == 0 {
                return None;
            }

            let r = self
                .inner()
                .strong
                .compare_exchange_weak(c, c + 1, Relaxed, Relaxed);

            match r {
                Ok(_) => {
                    return Some(Task {
                        p: self.p,
                        phantom: PhantomData,
                    })
                }
                Err(c2) => {
                    c = c2;
                }
            };
        }
    }
}

impl Drop for Weak {
    fn drop(&mut self) {
        if self.inner().weak.fetch_sub(1, Release) > 1 {
            return;
        }

        self.inner().weak.load(Acquire);

        drop(unsafe { Box::from_raw(self.p.as_ptr()) });
    }
}

impl Clone for Weak {
    fn clone(&self) -> Self {
        self.inner().weak.fetch_add(1, Relaxed);

        Self {
            p: self.p,
            phantom: PhantomData,
        }
    }
}

unsafe impl Sync for Weak {}
unsafe impl Send for Weak {}

//-----------------------------------------------------------------------------

pub struct TaskWaker {
    weak_ptr: Option<Weak>,
    sender: Sender<Weak>,
}

impl std::task::Wake for TaskWaker {
    fn wake(self: std::sync::Arc<Self>) {
        let s = self.sender.clone();
        if let Some(weak_ptr) = &self.weak_ptr {
            s.send(weak_ptr.clone()).unwrap();
        }
    }
}

//-----------------------------------------------------------------------------

struct IoContextFrame {
    tasks: HashSet<Task>,
    receiver: Receiver<Weak>,
    sender: Sender<Weak>,
    root_task: Option<Weak>,
}

//-----------------------------------------------------------------------------

pub struct IoContext {
    p: Rc<RefCell<IoContextFrame>>,
}

impl IoContext {
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();

        Self {
            p: Rc::new(RefCell::new(IoContextFrame {
                tasks: HashSet::new(),
                receiver: rx,
                sender: tx,
                root_task: None,
            })),
        }
    }

    #[must_use]
    pub fn get_executor(&self) -> Executor {
        Executor { p: self.p.clone() }
    }

    pub fn run(&mut self) -> u64 {
        let mut num_completed = 0;

        loop {
            if self.p.borrow().tasks.is_empty() {
                break;
            }

            let m_item = self.p.borrow().receiver.try_recv();
            if let Ok(ref w) = m_item {
                match w.upgrade() {
                    None => break,
                    Some(task) => {
                        (*self.p).borrow_mut().root_task = Some(w.clone());

                        let sender = self.p.borrow().sender.clone();

                        let w = Arc::new(TaskWaker {
                            weak_ptr: Some(w.clone()),
                            sender,
                        })
                        .into();

                        let mut cx = std::task::Context::from_waker(&w);
                        if let Poll::Ready(()) =
                            unsafe { Pin::new_unchecked(&mut *(*task.p.as_ptr()).future.get()) }
                                .poll(&mut cx)
                        {
                            (*self.p).borrow_mut().tasks.remove(&task);
                            num_completed += 1;
                        }
                    }
                }
            }
        }

        num_completed
    }
}

impl Default for IoContext {
    fn default() -> Self {
        Self::new()
    }
}

//-----------------------------------------------------------------------------

struct SpawnValue<T> {
    t: Option<T>,
    waker: Option<std::task::Waker>,
}

//-----------------------------------------------------------------------------

struct WrapperFuture<T, F: Future<Output = T>> {
    f: F,
    value: Rc<RefCell<SpawnValue<T>>>,
}

impl<T, F: Future<Output = T>> Future for WrapperFuture<T, F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        let r = unsafe { Pin::new_unchecked(&mut this.f).poll(cx) };
        match r {
            Poll::Pending => Poll::Pending,
            Poll::Ready(t) => {
                let state = &mut *(*this.value).borrow_mut();
                state.t = Some(t);
                if let Some(ref w) = state.waker {
                    w.wake_by_ref();
                }
                Poll::Ready(())
            }
        }
    }
}

//-----------------------------------------------------------------------------

pub struct SpawnFuture<T> {
    done: bool,
    value: Rc<RefCell<SpawnValue<T>>>,
}

impl<T> Future for SpawnFuture<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        assert!(!self.done);

        let result = self.value.borrow_mut().t.take();

        match result {
            None => {
                self.value.borrow_mut().waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some(t) => {
                self.done = true;
                Poll::Ready(t)
            }
        }
    }
}

//-----------------------------------------------------------------------------

#[derive(Clone)]
pub struct Executor {
    p: Rc<RefCell<IoContextFrame>>,
}

impl Executor {
    pub fn spawn<T: 'static, F: Future<Output = T> + 'static>(&self, f: F) -> SpawnFuture<T> {
        let value = Rc::new(RefCell::new(SpawnValue {
            t: None,
            waker: None,
        }));

        let sender = self.p.borrow().sender.clone();

        let wrapped = WrapperFuture {
            f,
            value: value.clone(),
        };

        let task = Task::new(wrapped);

        sender.send(Task::downgrade(&task)).unwrap();
        (*self.p).borrow_mut().tasks.insert(task);

        SpawnFuture { value, done: false }
    }
}
