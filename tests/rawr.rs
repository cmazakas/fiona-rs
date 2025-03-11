#![feature(vec_into_raw_parts)]

use std::{
    collections::VecDeque,
    mem,
    net::{Ipv4Addr, SocketAddrV4},
    os::fd::AsRawFd,
    panic, ptr,
    thread::spawn,
    time::{Duration, Instant},
};

use liburing_rs::{
    __kernel_timespec, IORING_RECVSEND_BUNDLE, IORING_RECVSEND_POLL_FIRST, IORING_SETUP_CQSIZE,
    IORING_SETUP_DEFER_TASKRUN, IORING_SETUP_SINGLE_ISSUER, IOSQE_BUFFER_SELECT,
    IOSQE_CQE_SKIP_SUCCESS, IOSQE_FIXED_FILE, IOSQE_IO_LINK, io_uring, io_uring_buf_ring_add,
    io_uring_buf_ring_advance, io_uring_buf_ring_mask, io_uring_cq_advance,
    io_uring_cqe_get_data64, io_uring_for_each_cqe, io_uring_get_sqe, io_uring_params,
    io_uring_prep_accept_direct, io_uring_prep_connect, io_uring_prep_link_timeout,
    io_uring_prep_recv_multishot, io_uring_prep_socket_direct, io_uring_queue_exit,
    io_uring_queue_init_params, io_uring_register_files_sparse, io_uring_register_files_update,
    io_uring_register_ring_fd, io_uring_setup_buf_ring, io_uring_sq_space_left, io_uring_sqe,
    io_uring_sqe_set_buf_group, io_uring_sqe_set_data, io_uring_sqe_set_data64,
    io_uring_sqe_set_flags, io_uring_submit, io_uring_submit_and_wait, io_uring_unregister_files,
};

use nix::{
    errno::Errno,
    libc::{AF_INET, IPPROTO_TCP, SOCK_STREAM},
    sys::socket::{
        AddressFamily, Backlog, SockFlag, SockProtocol, SockType, SockaddrIn, SockaddrStorage,
        bind, listen, setsockopt, socket, sockopt,
    },
};

const PORT: u16 = 8081;
const SQ_ENTRIES: u32 = 256;
const CQ_ENTRIES: u32 = 32 * 1024;
const NUM_BUFS: u32 = 16 * 1024;
const BUF_SIZE: u32 = 1024;
const TOTAL_CONNS: u32 = 512;

unsafe fn get_sqe(ring: *mut io_uring) -> *mut io_uring_sqe {
    let mut sqe = unsafe { io_uring_get_sqe(ring) };
    while sqe.is_null() {
        unsafe { io_uring_submit(ring) };
        sqe = unsafe { io_uring_get_sqe(ring) };
    }
    sqe
}

unsafe fn reserve_sqes(ring: *mut io_uring, n: u32) {
    if unsafe { io_uring_sq_space_left(ring) } < n {
        unsafe { io_uring_submit(ring) };
    }
}

#[repr(u16)]
enum OpType {
    TcpAccept = 0x0001,
    TcpRecv = 0x0002,
    Unknown = 0xffff,
}

struct IoObject {
    accept_fd: i32,
}

impl IoObject {
    fn new() -> IoObject {
        IoObject { accept_fd: -1 }
    }
}

fn make_user_data(fd: i32, op_type: OpType) -> u64 {
    let mut ud = 0_u64;
    ud |= fd as u64;
    ud |= (op_type as u64) << 32;
    ud
}

fn cqe_to_fd(user_data: u64) -> i32 {
    let ud = user_data & (u32::MAX as u64);
    ud.try_into().unwrap()
}

fn cqe_to_op_type(user_data: u64) -> OpType {
    let ud = ((user_data >> 32) & 0xffff_u64) as u16;
    match ud {
        0x0001 => OpType::TcpAccept,
        0x0002 => OpType::TcpRecv,
        _ => OpType::Unknown,
    }
}

#[derive(Clone, Copy)]
struct Acceptor {
    fd: i32,
}

unsafe fn prep_accept(
    ring: *mut io_uring,
    acceptor: Acceptor,
    fd_pool: &mut VecDeque<i32>,
    io_object_pool: &mut [IoObject],
) {
    let sqe = unsafe { get_sqe(ring) };
    let fd = acceptor.fd;
    let file_index = fd_pool.pop_front().unwrap();

    let idx = acceptor.fd as usize;
    let io_obj = &mut io_object_pool[idx];
    io_obj.accept_fd = file_index;

    unsafe {
        io_uring_prep_accept_direct(
            sqe,
            fd,
            ptr::null_mut(),
            ptr::null_mut(),
            0,
            file_index.try_into().unwrap(),
        );
    }
    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE) };
    unsafe { io_uring_sqe_set_data64(sqe, make_user_data(fd, OpType::TcpAccept)) };
}

unsafe fn prep_recv(ring: *mut io_uring, sockfd: i32, bgid: i32) {
    let sqe = unsafe { get_sqe(ring) };
    let bgid = -1;

    unsafe { io_uring_prep_recv_multishot(sqe, sockfd, ptr::null_mut(), 0, 0) };
    unsafe { io_uring_sqe_set_buf_group(sqe, bgid) };
    unsafe { io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE | IOSQE_BUFFER_SELECT) };
    unsafe {
        io_uring_sqe_set_data64(sqe, make_user_data(sockfd, OpType::TcpRecv));
    }

    let ioprio = (IORING_RECVSEND_POLL_FIRST | IORING_RECVSEND_BUNDLE) as u16;
    unsafe { (*sqe).ioprio |= ioprio };
}

fn client() {
    let start = Instant::now();

    let mut ring = unsafe { mem::zeroed::<io_uring>() };
    let ring = &raw mut ring;

    let nr_files = TOTAL_CONNS + 1;

    let mut fd_pool = VecDeque::with_capacity(nr_files.try_into().unwrap());
    {
        for i in 0..nr_files {
            fd_pool.push_back(i);
        }
    }

    {
        let cq_entries = CQ_ENTRIES;
        let sq_entries = SQ_ENTRIES;

        let mut params = unsafe { std::mem::zeroed::<io_uring_params>() };
        params.cq_entries = cq_entries;
        params.flags |= IORING_SETUP_CQSIZE;
        params.flags |= IORING_SETUP_SINGLE_ISSUER;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;

        let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, &raw mut params) };
        assert_eq!(ret, 0);

        let ret = unsafe { io_uring_register_files_sparse(ring, nr_files) };
        assert_eq!(ret, 0);

        let ret = unsafe { io_uring_register_ring_fd(ring) };
        assert_eq!(ret, 1);
    }

    let mut ts = __kernel_timespec {
        tv_sec: 3,
        tv_nsec: 0,
    };

    let ipv4_addr = Ipv4Addr::LOCALHOST;
    let port = PORT;
    let addr = SocketAddrV4::new(ipv4_addr, port);
    let addr: SockaddrStorage = addr.into();

    for _ in 0..TOTAL_CONNS {
        let fd = fd_pool.pop_front().unwrap();

        unsafe { reserve_sqes(ring, 3) };

        {
            let sqe = unsafe { get_sqe(ring) };
            unsafe { io_uring_prep_socket_direct(sqe, AF_INET, SOCK_STREAM, IPPROTO_TCP, fd, 0) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK) }
            unsafe { io_uring_sqe_set_data64(sqe, 0x02) };
        }

        {
            let sqe = unsafe { get_sqe(ring) };
            let addrlen = mem::size_of::<SockaddrIn>();

            unsafe {
                io_uring_prep_connect(
                    sqe,
                    fd.try_into().unwrap(),
                    std::ptr::from_ref(&addr).cast(),
                    addrlen.try_into().unwrap(),
                );
            }
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_IO_LINK | IOSQE_FIXED_FILE) };
            unsafe { io_uring_sqe_set_data64(sqe, 0x0102) };
        }

        {
            let ts = &raw mut ts;
            let sqe = unsafe { get_sqe(ring) };
            unsafe { io_uring_prep_link_timeout(sqe, ts.cast(), 0) };
            unsafe { io_uring_sqe_set_data(sqe, std::ptr::null_mut()) };
            unsafe { io_uring_sqe_set_flags(sqe, IOSQE_CQE_SKIP_SUCCESS) };
        }
    }

    let mut num_tasks = TOTAL_CONNS;
    while num_tasks > 0 {
        unsafe { io_uring_submit_and_wait(ring, 1) };

        let mut i = 0;

        unsafe {
            io_uring_for_each_cqe(ring, |cqe| {
                i += 1;

                let user_data = io_uring_cqe_get_data64(cqe);
                if user_data == 0x0102 {
                    // println!("completed tcp connect() call ({num_tasks})");
                    num_tasks -= 1;
                }

                if user_data == 0x02 {
                    // println!("created tcp socket");
                }
            })
        };

        unsafe { io_uring_cq_advance(ring, i) };
    }

    println!("client took: {:?}", start.elapsed());

    std::thread::sleep(Duration::from_secs(3));

    let ret = unsafe { io_uring_unregister_files(ring) };
    assert_eq!(ret, 0);

    unsafe { io_uring_queue_exit(ring) };
}

fn server() {
    let mut ring = unsafe { mem::zeroed::<io_uring>() };
    let ring = &raw mut ring;

    let nr_files = TOTAL_CONNS + 1;

    let mut fd_pool = VecDeque::<i32>::with_capacity(nr_files.try_into().unwrap());
    {
        for i in 0..nr_files {
            fd_pool.push_back(i as _);
        }
    }

    let mut io_object_pool = Vec::<IoObject>::with_capacity(nr_files.try_into().unwrap());
    {
        for _ in 0..nr_files {
            io_object_pool.push(IoObject::new());
        }
    }

    {
        let cq_entries = CQ_ENTRIES;
        let sq_entries = SQ_ENTRIES;

        let mut params = unsafe { std::mem::zeroed::<io_uring_params>() };
        params.cq_entries = cq_entries;
        params.flags |= IORING_SETUP_CQSIZE;
        params.flags |= IORING_SETUP_SINGLE_ISSUER;
        params.flags |= IORING_SETUP_DEFER_TASKRUN;

        let ret = unsafe { io_uring_queue_init_params(sq_entries, ring, &raw mut params) };
        assert_eq!(ret, 0);

        let ret = unsafe { io_uring_register_files_sparse(ring, nr_files) };
        assert_eq!(ret, 0);

        let ret = unsafe { io_uring_register_ring_fd(ring) };
        assert_eq!(ret, 1);
    }

    let bgid = 23;
    let buf_ring;
    {
        let mut ret = 0;
        let num_bufs = NUM_BUFS.next_power_of_two();

        buf_ring = unsafe { io_uring_setup_buf_ring(ring, num_bufs, bgid, 0, &mut ret) };
        if buf_ring.is_null() {
            panic!("{:?} aka {ret}", Errno::from_raw(-ret));
        }

        for bid in 0..num_bufs {
            let mask = unsafe { io_uring_buf_ring_mask(num_bufs) };
            let buf = vec![0_u8; BUF_SIZE as usize];
            let (addr, len, _) = buf.into_raw_parts();

            unsafe {
                io_uring_buf_ring_add(
                    buf_ring,
                    addr.cast(),
                    len.try_into().unwrap(),
                    bid as _,
                    mask,
                    bid as _,
                )
            };
        }

        unsafe { io_uring_buf_ring_advance(buf_ring, num_bufs.try_into().unwrap()) };
    }

    let mut acceptor = Acceptor { fd: -1 };

    {
        let socket = socket(
            AddressFamily::Inet,
            SockType::Stream,
            SockFlag::empty(),
            SockProtocol::Tcp,
        )
        .unwrap();

        setsockopt(&socket, sockopt::ReuseAddr, &true).unwrap();

        let port = PORT;
        let ipv4_addr = Ipv4Addr::LOCALHOST;
        let addr = SocketAddrV4::new(ipv4_addr, port);
        let addr: SockaddrIn = addr.into();
        let mut sock = socket.as_raw_fd();

        bind(sock, &addr).unwrap();
        listen(&socket, Backlog::new(1024).unwrap()).unwrap();

        let off = fd_pool.pop_front().unwrap();
        let files = &raw mut sock;
        let nr_files = 1;
        let ret = unsafe {
            io_uring_register_files_update(ring, off.try_into().unwrap(), files, nr_files)
        };
        assert_eq!(ret, nr_files.try_into().unwrap());

        acceptor.fd = off;
    }

    let start = Instant::now();

    unsafe { prep_accept(ring, acceptor, &mut fd_pool, &mut io_object_pool) };

    let mut num_accepted = 0;
    let mut num_tasks = TOTAL_CONNS;
    while num_tasks > 0 {
        unsafe { io_uring_submit_and_wait(ring, 1) };

        let mut i = 0;
        unsafe {
            io_uring_for_each_cqe(ring, |cqe| {
                i += 1;

                let user_data = io_uring_cqe_get_data64(cqe);

                let fd = cqe_to_fd(user_data);
                let op_type = cqe_to_op_type(user_data);

                match op_type {
                    OpType::TcpAccept => {
                        num_tasks -= 1;

                        assert_eq!((*cqe).res, 0);

                        num_accepted += 1;
                        let io_obj = &mut io_object_pool[fd as usize];
                        let sockfd = io_obj.accept_fd;

                        // println!("accepted tcp connection ({num_accepted})");
                        // println!("{:?}", *cqe);
                        // println!("accepted new connection on: {}", io_obj.accept_fd);

                        if num_accepted < TOTAL_CONNS {
                            prep_accept(ring, acceptor, &mut fd_pool, &mut io_object_pool);
                        }

                        prep_recv(ring, sockfd, bgid);
                    }
                    OpType::TcpRecv => {
                        unimplemented!("must implement tcp recv handling")
                    }
                    OpType::Unknown => {
                        unreachable!()
                    }
                }
            })
        };

        unsafe { io_uring_cq_advance(ring, i) };
    }

    println!("server took: {:?}", start.elapsed());

    let ret = unsafe { io_uring_unregister_files(ring) };
    assert_eq!(ret, 0);

    unsafe { io_uring_queue_exit(ring) };
}

#[test]
fn second_try() {
    let t = spawn(client);
    server();
    t.join().unwrap();
}
