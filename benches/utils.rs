// Copyright 2025-2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

use std::{hint::black_box, time::Instant};

pub fn run_once<F>(bench_name: &'static str, f: F) -> Result<(), String>
where
    F: FnOnce() -> Result<(), String>,
{
    let start = Instant::now();
    let result = black_box(f());
    let end = Instant::now();
    let duration = end - start;
    println!("{bench_name} completed in {duration:?}");
    result
}
