// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{path::Path, time::Duration};

use futures::poll;

#[test]
fn file_open() {
    let mut ioc = fiona::IoContext::new();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let path = Path::new("/tmp/rawr.txt");
            let _file = fiona::file::File::open(&ex, path).await.unwrap();
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}

#[test]
fn file_close_on_drop() {
    // Test that we successfully close files on Drop.

    const NUM_FILES: u32 = 16;

    let mut ioc = fiona::IoContext::builder().num_files(NUM_FILES).build();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();
            for i in 0..NUM_FILES {
                let file = fiona::file::File::open(&ex, &format!("/tmp/file_close_on_drop_{i}"))
                    .await
                    .unwrap();

                files.push(file);
            }

            let file =
                fiona::file::File::open(&ex, &format!("/tmp/file_close_on_drop_{}", NUM_FILES + 1))
                    .await;

            assert!(file.is_err());

            files.clear();

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            for i in 0..NUM_FILES {
                let file = fiona::file::File::open(&ex, &format!("/tmp/file_close_on_drop_{i}"))
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
fn file_close_on_future_drop() {
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
                    fiona::file::File::open(&ex, &format!("/tmp/file_close_on_future_drop_{i}"));
                let r = poll!(&mut task);
                assert!(r.is_pending());

                tasks.push(task);
            }

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            let file = fiona::file::File::open(
                &ex,
                &format!("/tmp/file_close_on_future_drop_{}", NUM_FILES + 1),
            )
            .await;

            assert!(file.is_err());

            tasks.clear();

            for i in 0..NUM_FILES {
                let file =
                    fiona::file::File::open(&ex, &format!("/tmp/file_close_on_future_drop_{i}"))
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
                    fiona::file::File::open(&ex, &format!("/tmp/file_open_eager_drop_cancel_{i}"));
                let r = poll!(&mut task);
                assert!(r.is_pending());

                fiona::time::sleep(&ex, Duration::from_micros(1)).await;
                drop(task);
            }

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            for i in 0..NUM_FILES {
                let file =
                    fiona::file::File::open(&ex, &format!("/tmp/file_open_eager_drop_cancel_{i}"))
                        .await
                        .unwrap();

                files.push(file);
            }
        }
    });

    let n = ioc.run();
    assert_eq!(n, 1);
}
