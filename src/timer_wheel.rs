// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// This code is a shameless ripoff of Tokio's approach, who we're using as our
// reference implementation. The Tokio authors deserve all credit for their
// genius in this implementation.

#![allow(clippy::struct_field_names)]
#![allow(unused)]

use std::{
    array::from_fn,
    ptr::{self, NonNull, null, null_mut},
    task::{LocalWaker, Poll},
    time::{Duration, Instant},
};

use crate::Executor;

const NUM_LEVELS: usize = 6;
const NUM_SLOTS: usize = 64;
const LEVEL_MULT: usize = 64;

const MAX_DURATION: u64 = (1 << (6 * NUM_LEVELS)) - 1;

//-----------------------------------------------------------------------------

fn level_for(elapsed: u64, when: u64) -> usize {
    // Obtain the most significant bit where `when` and `elapsed` diverge. This
    // difference is what determines which level/slot we place this timer into.
    // Mix in in the SLOT_MASK for the case when the xor produces 0, as that breaks
    // our ability to reason about the position of the signifcand.
    const MASK: u64 = (1 << 6) - 1;
    let mut masked = elapsed ^ when | MASK;

    if masked >= MAX_DURATION {
        masked = MAX_DURATION - 1;
    }

    let leading_zeros = masked.leading_zeros() as usize;
    let signifcand = 63 - leading_zeros;

    // Determine which 6-bit word we belong to.
    signifcand / NUM_LEVELS
}

fn slot_for(when: u64, level: usize) -> usize {
    // Because we choose the level for our timer based on which 6-bit word it falls
    // into, we remove all lower-order bits and base the slot on that 6-bit word.
    ((when >> (level * 6)) % NUM_SLOTS as u64) as usize
}

fn slot_range(level: usize) -> u64 {
    // Remember, each level's slot's duration is a power of 64.
    LEVEL_MULT.pow(level as u32) as u64
}

// The entire duration for a level is simply the sum of its slot's durations,
// which we just fold into multiplication here.
fn level_range(level: usize) -> u64 {
    LEVEL_MULT as u64 * slot_range(level)
}

//-----------------------------------------------------------------------------

struct TimerState {
    prev: *mut TimerState,
    next: *mut TimerState,
    waker: Option<LocalWaker>,
    deadline: u64,
    done: bool,
}

//-----------------------------------------------------------------------------

struct LinkedList {
    head: *mut TimerState,
    tail: *mut TimerState,
}

impl LinkedList {
    fn new() -> LinkedList {
        LinkedList {
            head: null_mut(),
            tail: null_mut(),
        }
    }

    unsafe fn push_front(&mut self, node: *mut TimerState) {
        unsafe { (*node).prev = null_mut() };

        if self.head.is_null() {
            unsafe { (*node).next = null_mut() };
            self.head = node;
            self.tail = node;
            return;
        }

        let old_head = self.head;
        self.head = node;

        unsafe { (*self.head).next = old_head };
        unsafe { (*old_head).prev = self.head };
    }

    unsafe fn pop_back(&mut self) -> *mut TimerState {
        let node = self.tail;
        if node.is_null() {
            return null_mut();
        }

        let prev_node = unsafe { (*node).prev };
        self.tail = prev_node;
        if self.tail.is_null() {
            self.head = null_mut();
        } else {
            unsafe { (*prev_node).next = null_mut() };
        }

        unsafe { (*node).next = null_mut() };
        unsafe { (*node).prev = null_mut() };

        node
    }
}

//-----------------------------------------------------------------------------

#[derive(Debug)]
struct Expiration {
    level: usize,
    slot: usize,
    deadline: u64,
}

//-----------------------------------------------------------------------------

struct Level {
    level: usize,
    occupied: u64,
    slots: [LinkedList; NUM_SLOTS],
}

impl Level {
    unsafe fn add_entry(&mut self, timer: *mut TimerState) {
        let t = unsafe { &mut *timer };
        let slot = slot_for(t.deadline, self.level);
        unsafe { self.slots[slot].push_front(timer) };
        self.occupied |= 1 << slot;
    }

    fn next_occupied_slot(&self, now: u64) -> Option<usize> {
        if self.occupied == 0 {
            return None;
        }

        let now_slot = (now / slot_range(self.level)) as usize;
        let occupied = self.occupied.rotate_right(now_slot as u32);
        let zeros = occupied.trailing_zeros() as usize;
        let slot = (zeros + now_slot) % LEVEL_MULT;

        Some(slot)
    }

    fn next_expiration(&self, now: u64) -> Option<Expiration> {
        let slot = self.next_occupied_slot(now)?;

        let level_range = level_range(self.level);
        let slot_range = slot_range(self.level);

        let level_start = now & !(level_range - 1);
        let mut deadline = level_start + slot as u64 * slot_range;

        if deadline <= now {
            assert_eq!(self.level, NUM_LEVELS - 1);
            deadline += level_range;
        }

        Some(Expiration {
            level: self.level,
            slot,
            deadline,
        })
    }
}

//-----------------------------------------------------------------------------

pub(crate) struct TimerWheel {
    elapsed: u64,
    levels: [Level; NUM_LEVELS],
}

impl TimerWheel {
    pub(crate) fn new() -> TimerWheel {
        let levels = from_fn(|i| Level {
            level: i,
            occupied: 0,
            slots: std::array::from_fn(|_| LinkedList::new()),
        });

        TimerWheel { elapsed: 0, levels }
    }

    unsafe fn add_timer(&mut self, timer: *mut TimerState) {
        let timer = unsafe { &mut *timer };
        assert!(timer.deadline > self.elapsed);

        let level = level_for(self.elapsed, timer.deadline);
        unsafe { self.levels[level].add_entry(timer) };
    }

    pub(crate) fn poll(&mut self, now: u64) {
        loop {
            match self.next_expiration() {
                Some(ref expiration) if expiration.deadline <= now => {
                    self.process_expiration(expiration);
                    self.set_elapsed(expiration.deadline);
                }
                _ => {
                    self.set_elapsed(now);
                    break;
                }
            }
        }
    }

    fn next_expiration(&self) -> Option<Expiration> {
        for level in &self.levels {
            if let Some(expiration) = level.next_expiration(self.elapsed) {
                return Some(expiration);
            }
        }

        None
    }

    pub(crate) fn next_expiration_time(&self) -> Option<u64> {
        self.next_expiration().map(|ex| ex.deadline)
    }

    fn process_expiration(&mut self, expiration: &Expiration) {
        let mut timer_list = std::mem::replace(
            &mut self.levels[expiration.level].slots[expiration.slot],
            LinkedList::new(),
        );
        self.levels[expiration.level].occupied &= !(1 << expiration.slot);

        while let timer = unsafe { timer_list.pop_back() }
            && !timer.is_null()
        {
            let timer = unsafe { &mut *timer };
            if timer.deadline <= expiration.deadline {
                timer.waker.as_ref().unwrap().wake_by_ref();
                timer.done = true;
            } else {
                let level = level_for(expiration.deadline, timer.deadline);
                unsafe { self.levels[level].add_entry(timer) };
            }
        }
    }

    fn elapsed(&self) -> u64 {
        self.elapsed
    }

    fn set_elapsed(&mut self, when: u64) {
        assert!(self.elapsed <= when);
        self.elapsed = when;
    }
}

//-----------------------------------------------------------------------------

struct TimerFuture {
    state: TimerState,
    ex: Executor,
    duration: Duration,
    initiated: bool,
    completed: bool,
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        assert!(!self.completed);
        match (self.initiated, self.state.done) {
            (true, false) => {
                self.state.waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
            (false, true) => unreachable!(),
            (true, true) => {
                self.completed = true;
                Poll::Ready(())
            }
            (false, false) => {
                let deadline = Instant::now() + self.duration;
                let dur_since = round_up_ms(deadline.duration_since(self.ex.p.wheel_start_time));

                let deadline: u64 = dur_since.as_millis().try_into().unwrap();
                self.state.deadline = deadline;

                let state = &raw mut self.state;
                unsafe { self.ex.p.timer_wheel.borrow_mut().add_timer(state) };
                self.initiated = true;
                self.state.waker = Some(cx.local_waker().clone());
                Poll::Pending
            }
        }
    }
}

//-----------------------------------------------------------------------------

pub fn sleep_for(ex: &Executor, duration: Duration) -> impl Future<Output = ()> {
    let duration = round_up_ms(duration);
    TimerFuture {
        state: TimerState {
            prev: null_mut(),
            next: null_mut(),
            waker: None,
            deadline: 0,
            done: false,
        },
        ex: ex.clone(),
        duration,
        initiated: false,
        completed: false,
    }
}

//-----------------------------------------------------------------------------

pub(crate) fn round_up_ms(dur: Duration) -> Duration {
    let s = dur.as_secs();
    let mut ms = dur.subsec_millis();
    let us = dur.subsec_micros();
    if us == ms * 1_000 {
        return dur;
    }
    Duration::new(s, (ms + 1) * 1_000_000)
}

//-----------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use std::{
        cell::RefCell,
        collections::VecDeque,
        rc::Rc,
        task::{Context, LocalWake, Waker},
        time::Instant,
    };

    use rand::{Rng, SeedableRng, TryRngCore, rngs::OsRng};

    use super::*;

    // Copy Tokio's test from here:
    // https://github.com/tokio-rs/tokio/blob/be689a35f5ade5a39e507f79d3ec85cdab27806f/tokio/src/runtime/time/wheel/mod.rs#L293-L332
    #[test]
    fn test_level_for() {
        for pos in 0..64 {
            assert_eq!(0, level_for(0, pos), "level_for({pos}) -- binary = {pos:b}");
        }

        for level in 1..5 {
            for pos in level..64 {
                let a = pos * 64_usize.pow(level as u32);
                assert_eq!(level, level_for(0, a as u64), "level_for({a}) -- binary = {a:b}");

                if pos > level {
                    let a = a - 1;
                    assert_eq!(level, level_for(0, a as u64), "level_for({a}) -- binary = {a:b}");
                }

                if pos < 64 {
                    let a = a + 1;
                    assert_eq!(level, level_for(0, a as u64), "level_for({a}) -- binary = {a:b}");
                }
            }
        }
    }

    #[test]
    fn test_level_for2() {
        assert_eq!(0, level_for(1000, 1001));
        assert_eq!(0, level_for(1000, 1023));
        assert_eq!(1, level_for(1000, 1024));
    }

    // Copy Tokio's test from here:
    // https://github.com/tokio-rs/tokio/blob/be689a35f5ade5a39e507f79d3ec85cdab27806f/tokio/src/runtime/time/wheel/level.rs#L180-L192
    #[test]
    fn test_slot_for() {
        for pos in 0..64 {
            assert_eq!(pos as usize, slot_for(pos, 0));
        }

        for level in 1..5 {
            for pos in level..64 {
                let a = pos * 64_usize.pow(level as u32);
                assert_eq!(pos, slot_for(a as u64, level));
            }
        }
    }

    #[test]
    fn test_slot_for2() {
        assert_eq!(0, slot_for(960, 0));
        assert_eq!(1, slot_for(961, 0));
        assert_eq!(63, slot_for(1023, 0));
    }

    #[test]
    fn timer_wheel_add_timer() {
        let mut timer_wheel = TimerWheel::new();

        let mut cx = Context::from_waker(Waker::noop());

        let mut timer1 = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 13,
            done: false,
        };

        let mut timer2 = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            done: false,
            deadline: 27,
        };

        let mut timer3 = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            done: false,
            deadline: 63,
        };

        unsafe { timer_wheel.add_timer(&raw mut timer1) };
        unsafe { timer_wheel.add_timer(&raw mut timer2) };
        unsafe { timer_wheel.add_timer(&raw mut timer3) };

        let mut timer1_copy = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 13,
            done: false,
        };

        let mut timer2_copy = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 27,
            done: false,
        };

        let mut timer3_copy = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 63,
            done: false,
        };

        unsafe { timer_wheel.add_timer(&raw mut timer1_copy) };
        unsafe { timer_wheel.add_timer(&raw mut timer2_copy) };
        unsafe { timer_wheel.add_timer(&raw mut timer3_copy) };
    }

    struct TestWaker {
        queue: Rc<RefCell<Vec<i32>>>,
        x: i32,
    }

    impl LocalWake for TestWaker {
        fn wake(self: Rc<Self>) {
            self.queue.borrow_mut().push(self.x);
        }
    }

    fn make_timer(deadline: u64, x: i32, queue: Rc<RefCell<Vec<i32>>>) -> TimerState {
        let local_waker = Rc::new(TestWaker { queue, x }).into();

        let mut cx = std::task::ContextBuilder::from_waker(std::task::Waker::noop())
            .local_waker(&local_waker)
            .build();

        TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline,
            done: false,
        }
    }

    #[test]
    fn timer_wheel_poll() {
        let mut timer_wheel = TimerWheel::new();

        let queue = Rc::new(RefCell::new(Vec::new()));

        {
            let mut timer1 = make_timer(13, 1, queue.clone());

            unsafe { timer_wheel.add_timer(&raw mut timer1) };

            timer_wheel.poll(4);
            assert!(queue.borrow().is_empty());

            timer_wheel.poll(14);
            assert_eq!(&queue.borrow()[..], [1]);
        }

        queue.borrow_mut().clear();
        assert_eq!(timer_wheel.elapsed(), 14);

        {
            let mut timer1 = make_timer(14 + 13, 1, queue.clone());
            unsafe { timer_wheel.add_timer(&raw mut timer1) };

            let mut timer1 = make_timer(14 + 64, 2, queue.clone());
            unsafe { timer_wheel.add_timer(&raw mut timer1) };

            let mut timer1 = make_timer(14 + 1234, 3, queue.clone());
            unsafe { timer_wheel.add_timer(&raw mut timer1) };

            let mut timer1 = make_timer(14 + 1264, 4, queue.clone());
            unsafe { timer_wheel.add_timer(&raw mut timer1) };

            timer_wheel.poll(14 + 3);
            assert!(queue.borrow().is_empty());

            timer_wheel.poll(14 + 11);
            assert!(queue.borrow().is_empty());

            timer_wheel.poll(14 + 14);
            assert_eq!(&queue.borrow()[..], [1]);

            timer_wheel.poll(14 + 63);
            assert_eq!(&queue.borrow()[..], [1]);

            timer_wheel.poll(14 + 64);
            assert_eq!(&queue.borrow()[..], [1, 2]);

            timer_wheel.poll(14 + 1270);
            assert_eq!(&queue.borrow()[..], [1, 2, 3, 4]);
        }
    }

    #[test]
    fn timer_wheel_poll_fuzzy() {
        let mut timer_wheel = TimerWheel::new();

        let queue = Rc::new(RefCell::new(Vec::<i32>::new()));

        let mut timers = Vec::new();

        let mut max_deadline = 0;

        let mut expected = Vec::<(i32, usize)>::new();

        let mut i = 0;
        for level in 0..NUM_LEVELS {
            for slot in 0..NUM_SLOTS {
                for j in 0..3 {
                    let deadline =
                        LEVEL_MULT.pow(level as u32) + LEVEL_MULT.pow(level as u32) * slot + j;

                    let deadline = deadline.min(MAX_DURATION as usize);
                    timers.push(Box::new(make_timer(deadline as u64, i, queue.clone())));
                    unsafe { timer_wheel.add_timer(Box::as_mut_ptr(timers.last_mut().unwrap())) };

                    expected.push((i, deadline));

                    i += 1;

                    max_deadline = deadline;
                }
            }
        }

        let mut rng = rand::rngs::StdRng::from_os_rng();

        let mut deadline = 0;
        while deadline < max_deadline {
            let remaining = max_deadline - deadline;
            let elapsed = rng.random_range(1_u64..remaining as u64 + 1);
            let now = deadline as u64 + elapsed;

            timer_wheel.poll(now);
            deadline += elapsed as usize;
        }
        timer_wheel.poll(max_deadline as u64 + 1);

        assert!(timer_wheel.elapsed() > max_deadline as u64);

        // Sort our indices by the deadlines we've ascribed them and if required, sort
        // them by the indices themselves. Testing against this property ensures we
        // follow a proper FIFO structure, which requires pop_front() and pop_back().
        expected.sort_by(|lhs, rhs| {
            if lhs.1 == rhs.1 {
                return lhs.0.cmp(&rhs.0);
            }

            lhs.1.cmp(&rhs.1)
        });

        let expected = expected
            .iter()
            .map(|(idx, deadline)| *idx)
            .collect::<Vec<_>>();

        assert_eq!(*queue.borrow(), expected);
    }

    #[test]
    fn timer_wheel_round_up_ms() {
        let test_cases = vec![
            (1_000_500_000, 1_001_000_000),
            (1_456_400_000, 1_457_000_000),
            (1_456_004_000, 1_457_000_000),
            (1_456_000_000, 1_456_000_000),
            (1_456_000_123, 1_456_000_123),
        ];

        for &(x, expected) in &test_cases {
            assert_eq!(round_up_ms(Duration::from_nanos(x)), Duration::from_nanos(expected));
        }
    }
}
