use std::{ffi::c_void, time::Instant};

use liburing_rs::{
    IORING_SETUP_CQSIZE, IORING_SETUP_DEFER_TASKRUN, IORING_SETUP_SINGLE_ISSUER, io_uring,
    io_uring_cq_advance, io_uring_for_each_cqe, io_uring_get_sqe, io_uring_params,
    io_uring_prep_msg_ring, io_uring_prep_read, io_uring_queue_exit, io_uring_queue_init_params,
    io_uring_register_ring_fd, io_uring_register_sync_msg, io_uring_sqe, io_uring_sqe_set_data64,
    io_uring_submit_and_get_events, io_uring_submit_and_wait,
};

extern crate liburing_rs;
extern crate nix;

const SQ_ENTRIES: u32 = 256;
const CQ_ENTRIES: u32 = 32 * 1024;
const NUM_OPS: u64 = 1_000_000;
const NUM_THREADS: u64 = 10;

fn make_ring() -> *mut io_uring
{
    let mut params = unsafe { std::mem::zeroed::<io_uring_params>() };
    params.sq_entries = SQ_ENTRIES;
    params.cq_entries = CQ_ENTRIES;
    params.flags |= IORING_SETUP_CQSIZE;
    params.flags |= IORING_SETUP_SINGLE_ISSUER;
    params.flags |= IORING_SETUP_DEFER_TASKRUN;

    let ring = Box::into_raw(Box::new(unsafe { std::mem::zeroed::<io_uring>() }));

    let ret = unsafe { io_uring_queue_init_params(SQ_ENTRIES, ring, &raw mut params) };
    assert_eq!(ret, 0);

    let ret = unsafe { io_uring_register_ring_fd(ring) };
    assert_eq!(ret, 1);

    ring
}

unsafe fn submit_ring(ring: *mut io_uring)
{
    let _r = unsafe { io_uring_submit_and_get_events(ring) };
}

unsafe fn get_sqe(ring: *mut io_uring) -> *mut io_uring_sqe
{
    let mut sqe = unsafe { io_uring_get_sqe(ring) };
    while sqe.is_null() {
        unsafe { submit_ring(ring) };
        sqe = unsafe { io_uring_get_sqe(ring) };
    }
    sqe
}

fn eventfd()
{
    use std::os::fd::AsRawFd;

    let ring = make_ring();

    let event_fd = nix::sys::eventfd::EventFd::new().unwrap();
    let event_fd = event_fd.as_raw_fd();

    let mut n = 0_u64;
    let mut event_count = 0_u64;

    let (tx, rx) = std::sync::mpsc::channel::<u64>();

    let mut threads = Vec::new();
    for _ in 0..NUM_THREADS {
        let tx = tx.clone();
        threads.push(std::thread::spawn(move || {
                         let buf = &0x01_u64.to_ne_bytes();
                         for _ in 0..NUM_OPS / NUM_THREADS {
                             unsafe {
                                 nix::libc::write(event_fd,
                                                  buf.as_ptr().cast::<c_void>(),
                                                  buf.len());
                             }
                             tx.send(1).unwrap();
                         }
                     }));
    }

    let start = Instant::now();

    while n < NUM_OPS {
        let sqe = unsafe { get_sqe(ring) };
        let p = std::ptr::from_mut(&mut event_count).cast::<c_void>();

        unsafe { io_uring_prep_read(sqe, event_fd, p, 8, 0) };
        unsafe { io_uring_sqe_set_data64(sqe, 0x01) };

        let _ret = unsafe { io_uring_submit_and_wait(ring, 1) };

        unsafe {
            let mut count = 0;
            io_uring_for_each_cqe(ring, |_cqe| {
                count += 1;
            });
            io_uring_cq_advance(ring, count);
        }

        while let Ok(v) = rx.try_recv() {
            n += v;
        }
    }

    let end = Instant::now();

    println!("eventfd version took: {:?}", end - start);

    unsafe { io_uring_queue_exit(ring) };
    let _ = unsafe { Box::from_raw(ring) };

    for thread in threads {
        thread.join().unwrap();
    }
}

fn ring_messaging()
{
    let ring = make_ring();

    let mut n = 0;

    let ring_fd = unsafe { (*ring).ring_fd };

    let mut threads = Vec::new();
    for _ in 0..NUM_THREADS {
        threads.push(std::thread::spawn(move || {
                         for _ in 0..NUM_OPS / NUM_THREADS {
                             let mut sqe = unsafe { std::mem::zeroed::<io_uring_sqe>() };
                             unsafe { io_uring_prep_msg_ring(&mut sqe, ring_fd, 0x01, 0, 0) };
                             unsafe { io_uring_register_sync_msg(&mut sqe) };
                         }
                     }));
    }

    let start = Instant::now();

    while n < NUM_OPS {
        let _ret = unsafe { io_uring_submit_and_wait(ring, 1) };
        let mut count = 0;
        unsafe {
            io_uring_for_each_cqe(ring, |_cqe| {
                n += 1;
                count += 1;
            });
            io_uring_cq_advance(ring, count);
        }
    }

    let end = Instant::now();

    println!("ring messaging version took: {:?}", end - start);

    unsafe { io_uring_queue_exit(ring) };
    let _ = unsafe { Box::from_raw(ring) };

    for thread in threads {
        thread.join().unwrap();
    }
}

fn main()
{
    eventfd();
    ring_messaging();
}
