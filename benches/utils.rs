use std::{hint::black_box, time::Instant};

pub fn run_once<F>(bench_name: &'static str, mut f: F) -> Result<(), String>
    where F: FnMut() -> Result<(), String>
{
    let start = Instant::now();
    let result = black_box(f());
    let end = Instant::now();
    let duration = end - start;
    println!("{bench_name} completed in {duration:?}");
    result
}
