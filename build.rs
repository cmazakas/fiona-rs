// Copyright 2024 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{env, path::PathBuf};

extern crate bindgen;
extern crate pkg_config;

fn main() {
    let liburing_pkg = pkg_config::Config::new()
        .atleast_version("2.9")
        .cargo_metadata(false)
        .probe("liburing-ffi")
        .unwrap();

    liburing_pkg
        .link_paths
        .iter()
        .for_each(|path_buf| println!("cargo::rustc-link-search={}", path_buf.to_str().unwrap()));

    println!("cargo::rustc-link-lib=static=uring-ffi");

    let bindings = bindgen::Builder::default()
        .clang_args(
            liburing_pkg
                .include_paths
                .iter()
                .map(|path_buf| format!("-I{}", path_buf.to_str().unwrap())),
        )
        .clang_arg("-std=c11")
        .clang_arg("-D_POSIX_C_SOURCE=200809L")
        .header("include/liburing_wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("liburing_bindings.rs"))
        .expect("Couldn't write bindings!");
}
