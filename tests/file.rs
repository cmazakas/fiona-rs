// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{path::Path, time::Duration};

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

    let mut ioc = fiona::IoContext::builder().num_files(16).build();
    let ex = ioc.get_executor();

    ex.spawn({
        let ex = ex.clone();
        async move {
            let mut files = Vec::new();
            for i in 0..16 {
                let file = fiona::file::File::open(&ex, &format!("/tmp/file_close_on_drop_{i}"))
                    .await
                    .unwrap();

                files.push(file);
            }

            let file =
                fiona::file::File::open(&ex, &format!("/tmp/file_close_on_drop_{}", 16 + 1)).await;

            assert!(file.is_err());

            files.clear();

            fiona::time::sleep(&ex, Duration::from_millis(100)).await;

            for i in 0..16 {
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
