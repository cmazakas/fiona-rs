use std::{
    sync::{
        atomic::{AtomicI32, Ordering::Relaxed},
        Arc,
    },
    time::{Duration, Instant},
};

async fn timer_op(ex: fiona::Executor, anums: Arc<AtomicI32>) {
    let mut timer = fiona::time::Timer::new(ex);
    for _ in 0..10_000 {
        assert!(timer.wait(Duration::from_millis(1)).await.is_ok());
        anums.fetch_add(1, Relaxed);
    }
}

fn main() {
    let prev = Instant::now();

    let params = fiona::IoContextParams {
        sq_entries: 16 * 1024,
        cq_entries: 16 * 1024,
        ..Default::default()
    };

    let mut ioc = fiona::IoContext::with_params(&params);
    let ex = ioc.get_executor();

    let anums = Arc::new(AtomicI32::new(0));
    for _ in 0..10_000 {
        ex.spawn(timer_op(ex.clone(), anums.clone()));
    }

    ioc.run();
    assert_eq!(anums.load(Relaxed), 10_000 * 10_000);

    let dur = Instant::now().duration_since(prev);
    println!("duration: {dur:?}");
}