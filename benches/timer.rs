mod utils;

use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering::Relaxed},
    },
    time::Duration,
};

async fn timer_op(ex: fiona::Executor, anums: Arc<AtomicI32>) {
    let timer = fiona::time::Timer::new(ex);
    for _ in 0..10_000 {
        assert!(timer.wait(Duration::from_millis(1)).await.is_ok());
        anums.fetch_add(1, Relaxed);
    }
}

fn fiona_timer() -> Result<(), String> {
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

    Ok(())
}

fn main() {
    utils::run_once("fiona_timer", fiona_timer).unwrap();
}
