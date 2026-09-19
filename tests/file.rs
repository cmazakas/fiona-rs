// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{
    future::poll_fn,
    path::Path,
    pin::{Pin, pin},
    task::{Context, Poll, Waker},
    time::Duration,
};

use futures::{Stream, StreamExt, poll, stream::FuturesUnordered};
use rand::SeedableRng;

#[test]
fn file_open() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let path = Path::new("/tmp/rawr.txt");
            let _file = fiona::fs::File::open(&ex, path).await.unwrap();
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_open_close_on_drop() {
    // Test that we successfully close files on Drop.

    const NUM_FILES: u32 = 16;

    let mut ioc = fiona::IoContext::builder().num_files(NUM_FILES).build();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();
            for i in 0..NUM_FILES {
                let file = fiona::fs::File::open(&ex, &format!("/tmp/file_close_on_drop_{i}"))
                    .await
                    .unwrap();

                files.push(file);
            }

            let file =
                fiona::fs::File::open(&ex, &format!("/tmp/file_close_on_drop_{}", NUM_FILES + 1))
                    .await;

            assert!(file.is_err());

            files.clear();

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            for i in 0..NUM_FILES {
                let file = fiona::fs::File::open(&ex, &format!("/tmp/file_close_on_drop_{i}"))
                    .await
                    .unwrap();

                files.push(file);
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_open_close_on_future_drop() {
    // Test that our file is properly closed when the Future contains a success
    // result, but has not yet been polled to completion.

    const NUM_FILES: u32 = 16;

    let mut ioc = fiona::IoContext::builder().num_files(NUM_FILES).build();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();
            let mut tasks = Vec::new();

            for i in 0..NUM_FILES {
                let mut task =
                    fiona::fs::File::open(&ex, &format!("/tmp/file_close_on_future_drop_{i}"));
                let r = poll!(&mut task);
                assert!(r.is_pending());

                tasks.push(task);
            }

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            let file = fiona::fs::File::open(
                &ex,
                &format!("/tmp/file_close_on_future_drop_{}", NUM_FILES + 1),
            )
            .await;

            assert!(file.is_err());

            tasks.clear();

            for i in 0..NUM_FILES {
                let file =
                    fiona::fs::File::open(&ex, &format!("/tmp/file_close_on_future_drop_{i}"))
                        .await
                        .unwrap();

                files.push(file);
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_open_eager_drop_cancel() {
    // Test that we attempt to cancel our open() call if the Future is
    // eager-dropped.

    const NUM_FILES: u32 = 16;

    let mut ioc = fiona::IoContext::builder().num_files(NUM_FILES).build();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();

            for i in 0..NUM_FILES {
                let mut task =
                    fiona::fs::File::open(&ex, &format!("/tmp/file_open_eager_drop_cancel_{i}"));
                let r = poll!(&mut task);
                assert!(r.is_pending());

                fiona::time::sleep(&ex, Duration::from_micros(1)).await;
                drop(task);
            }

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            for i in 0..NUM_FILES {
                let file =
                    fiona::fs::File::open(&ex, &format!("/tmp/file_open_eager_drop_cancel_{i}"))
                        .await
                        .unwrap();

                files.push(file);
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_write() {
    // Test that we can successfully write continuously to a file.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "/tmp/fiona_test_file_write.txt";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            ex.register_fixed_buffers(8, 1024).unwrap();

            let mut message = vec![0; 8 * 1024];
            {
                let mut rng = rand::rngs::StdRng::from_os_rng();
                rand::RngCore::fill_bytes(&mut rng, &mut message);
            }

            // For this test, we exercise a code path where we grab multiple
            // registered buffers up front and then incrementally
            // consume each one.
            let mut fixed_bufs = Vec::new();
            {
                let mut msg = &message[..];
                for i in 0..8 {
                    let mut buf = ex.get_fixed_buf().unwrap();
                    assert_eq!(buf.buf_idx(), i as _);

                    let (to_write, remaining) = msg.split_at(1024);
                    msg = remaining;

                    buf.as_mut_slice().copy_from_slice(to_write);
                    fixed_bufs.push(buf);
                }
            }

            for buf in fixed_bufs {
                let (written, b) = file.write_at(buf, -1 as _).await;
                assert_eq!(written.unwrap(), b.len());
            }

            let content = std::fs::read(pathname).unwrap();
            assert_eq!(message, content);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_subspan_write() {
    // Test that we can successfully write continuously to a file using
    // subspans.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "/tmp/fiona_test_file_subspan_write";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            ex.register_fixed_buffers(8, 8 * 1024).unwrap();

            let mut message = vec![0; 8 * 1024];
            {
                let mut rng = rand::rngs::StdRng::from_os_rng();
                rand::RngCore::fill_bytes(&mut rng, &mut message);
            }

            // For this test, we exercise the codepath where we have one large
            // fixed buffer that we incrementally write from using
            // subspans.
            let mut buf = ex.get_fixed_buf().unwrap();
            buf.copy_from_slice(&message[..]);

            let mut written = 0;
            while written < message.len() {
                let (n, b) = file
                    .write_subspan_at(written..written + 1024, buf, -1 as _)
                    .await;
                buf = b;

                let n = n.unwrap();
                assert_eq!(n, 1024);
                written += n;
            }

            let content = std::fs::read(pathname).unwrap();
            assert_eq!(message, content);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_offset_out_of_bounds() {
    // Test that our code can handle sparse writes.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.register_fixed_buffers(8, 8 * 1024).unwrap();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "/tmp/fiona_test_file_offset_out_of_bounds";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            let mut buf = ex.get_fixed_buf().unwrap();
            let msg = "hello, world!";
            let n = msg.len();
            buf[..n].copy_from_slice(msg.as_bytes());

            let (written, _buf) = file.write_subspan_at(..n, buf, -1 as _).await;
            buf = _buf;
            assert_eq!(written.unwrap(), msg.len());

            let (n, _buf) = file.write_subspan_at(..n, buf, 1234).await;
            assert_eq!(n.unwrap(), msg.len());
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_eager_drop_write() {
    // Test that our code can handle eager dropped writes.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.register_fixed_buffers(16, 8 * 1024).unwrap();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();

            for i in 0..16 {
                let file = fiona::fs::File::open(
                    &ex,
                    format!("/tmp/fiona_test_file_eager_drop_write_{}", i),
                )
                .await
                .unwrap();

                files.push(file);
            }

            let msg = vec![127_u8; 4 * 1024];
            let n = msg.len();

            let mut cx = Context::from_waker(Waker::noop());

            let mut bufs = Vec::new();
            for _ in 0..16 {
                let mut buf = ex.get_fixed_buf().unwrap();
                buf[..n].copy_from_slice(&msg);
                bufs.push(buf);
            }

            let mut tasks = Vec::new();

            for (i, buf) in bufs.into_iter().enumerate() {
                let task = files[i].write_subspan_at(..n, buf, -1 as _);
                tasks.push(task);
            }

            for mut task in &mut tasks {
                assert!(Pin::new(&mut task).poll(&mut cx).is_pending());
            }

            drop(tasks);

            fiona::time::sleep(&ex, Duration::from_millis(250)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_read() {
    // Elementary test of file reading.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "/tmp/fiona_test_file_read.txt";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            ex.register_fixed_buffers(16, 1024).unwrap();

            let mut message = vec![0; 8 * 1024];
            {
                let mut rng = rand::rngs::StdRng::from_os_rng();
                rand::RngCore::fill_bytes(&mut rng, &mut message);
            }

            let mut fixed_bufs = Vec::new();
            {
                let mut msg = &message[..];
                for i in 0..8 {
                    let mut buf = ex.get_fixed_buf().unwrap();
                    assert_eq!(buf.buf_idx(), i as _);

                    let (to_write, remaining) = msg.split_at(1024);
                    msg = remaining;

                    buf.as_mut_slice().copy_from_slice(to_write);
                    fixed_bufs.push(buf);
                }
            }

            for buf in fixed_bufs {
                let (written, b) = file.write_at(buf, -1 as _).await;
                assert_eq!(written.unwrap(), b.len());
            }

            let content = std::fs::read(pathname).unwrap();
            assert_eq!(message, content);

            // Now read back what we wrote.
            let mut total = 0;

            let mut assembled_msg = Vec::new();
            for _ in 0..8 {
                let buf = ex.get_fixed_buf().unwrap();

                let (n, buf) = file.read_at(buf, total).await;
                assert!(n.is_ok());

                let n = n.unwrap();
                total += n as u64;

                assembled_msg.extend_from_slice(&buf[..n]);
            }

            assert_eq!(total, message.len() as u64);
            assert_eq!(assembled_msg, message);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_read_eager_drop() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "tests/file.rs";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            ex.register_fixed_buffers(16, 1024).unwrap();

            {
                let mut f = pin!(file.read_at(ex.get_fixed_buf().unwrap(), -1 as _));
                poll_fn(|cx| {
                    assert!(f.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;

                fiona::time::sleep(&ex, Duration::from_micros(100)).await;
            }

            // Helps make sure we cleanup the stray CQEs and that cancellation
            // is observed.
            fiona::time::sleep(&ex, Duration::from_millis(100)).await;
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_read_incremental() {
    // Test incremental reading of a file.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "src/lib.rs";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            let md = std::fs::metadata(pathname).unwrap();
            let file_size = md.len() as usize;

            ex.register_fixed_buffers(8, file_size.next_power_of_two() as _)
                .unwrap();

            let mut total = 0_usize;
            let mut buf = ex.get_fixed_buf().unwrap();

            let mut contents = Vec::new();

            while total < file_size {
                let (n, b) = if total + 512 >= file_size {
                    file.read_subspan_at(total.., buf, total as u64).await
                } else {
                    file.read_subspan_at(total..(total + 512), buf, total as u64)
                        .await
                };

                let n = n.unwrap();
                buf = b;

                contents.extend_from_slice(&buf[total..(total + n)]);
                total += n;
            }

            let expected = std::fs::read_to_string(pathname).unwrap();
            let contents = String::from_utf8(contents).unwrap();

            assert!(!expected.is_empty());
            assert_eq!(contents, expected);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_read_concurrent() {
    // Test concurrent reading of a file.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "src/lib.rs";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();

            let md = std::fs::metadata(pathname).unwrap();
            let file_size = md.len() as usize;
            let buf_len = 512;

            ex.register_fixed_buffers(
                (file_size / buf_len + 1).next_power_of_two() as u32,
                buf_len,
            )
            .unwrap();

            let mut joinset = FuturesUnordered::new();
            let mut contents = vec![0_u8; file_size];
            let num_reads = file_size.div_ceil(buf_len);
            for i in 0..num_reads {
                joinset.push(
                    file.read_at(ex.get_fixed_buf().unwrap(), (buf_len * i).try_into().unwrap()),
                );
            }

            while let Some((n, buf)) = joinset.next().await {
                let n = n.unwrap();
                let idx = buf.buf_idx();
                let s = idx * buf_len;
                let e = s + n;
                contents[s..e].copy_from_slice(&buf[..n]);
            }

            let expected = std::fs::read_to_string(pathname).unwrap();
            let contents = String::from_utf8(contents).unwrap();

            assert!(!expected.is_empty());
            assert_eq!(contents, expected);
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_close() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "src/lib.rs";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();
            ex.register_fixed_buffers(128, 512 * 1024).unwrap();

            let h = ex.spawn({
                let file = file.clone();
                async move {
                    file.close().await.unwrap();
                }
            });

            let (n, _buf) = file.read_at(ex.get_fixed_buf().unwrap(), -1 as _).await;
            assert!(n.unwrap() > 0);

            h.await;

            let (n, _buf) = file.read_at(ex.get_fixed_buf().unwrap(), 0).await;
            assert!(n.is_err());
        }
    });

    let n = ioc.run();
    assert_eq!(n, 2);
}

#[test]
fn file_cancel() {
    // This test only _attempts_ cancellation and never actually hits it. This
    // is due how io_uring interacts with the underlying file system and how
    // fast it is. In theory, cancellation is possible here but it's basically
    // impossible to trigger.

    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let pathname = "src/lib.rs";

            let file = fiona::fs::File::open(&ex, pathname).await.unwrap();
            ex.register_fixed_buffers(128, 1024).unwrap();

            let mut joinset = FuturesUnordered::new();
            for i in 0..128 {
                joinset.push(file.read_at(ex.get_fixed_buf().unwrap(), i as _));
            }

            poll_fn(|cx| {
                for _ in 0..128 {
                    assert!(Pin::new(&mut joinset).poll_next(cx).is_pending());
                }
                Poll::Ready(())
            })
            .await;

            file.cancel().await.unwrap();

            while let Some((n, _buf)) = joinset.next().await {
                assert!(n.is_ok());
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}
