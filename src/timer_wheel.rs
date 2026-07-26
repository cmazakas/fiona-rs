// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// This code is a shameless ripoff of Tokio's approach, who we're using as our
// reference implementation. The Tokio authors deserve all credit for their
// genius in this implementation.

#![allow(clippy::struct_field_names)]
#![allow(unused)]

use std::{array::from_fn, ptr, task::LocalWaker};

const NUM_LEVELS: usize = 6;
const NUM_SLOTS: usize = 64;

const MAX_DURATION: u64 = (1 << (6 * NUM_LEVELS)) - 1;

fn level_for(elapsed: u64, when: u64) -> usize {
    // Obtain the most significant bit where `when` and `elapsed` diverge. This
    // difference is what determines which level/slot we place this timer into.
    // Mixin in the SLOT_MASK for the case when the xor produces 0, as that breaks
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

struct TimerState {
    prev: *mut TimerState,
    next: *mut TimerState,
    waker: Option<LocalWaker>,
    deadline: u64,
}

struct Level {
    level: usize,
    occupied: u64,
    slots: [*mut TimerState; NUM_SLOTS],
}

struct TimerWheel {
    elapsed: u64,
    levels: [Level; NUM_LEVELS],
}

impl TimerWheel {
    fn new() -> TimerWheel {
        let levels = from_fn(|i| Level {
            level: i,
            occupied: 0,
            slots: [ptr::null_mut(); NUM_SLOTS],
        });

        TimerWheel { elapsed: 0, levels }
    }

    unsafe fn add_timer(&mut self, timer: *mut TimerState) {
        let timer = unsafe { &mut *timer };
        assert!(timer.deadline > self.elapsed);

        let level = level_for(self.elapsed, timer.deadline);
        let slot = slot_for(timer.deadline, level);

        let curr = self.levels[level].slots[slot];
        if !curr.is_null() {
            unsafe { (*curr).prev = timer };
        }

        timer.next = curr;
        self.levels[level].slots[slot] = timer;
        self.levels[level].occupied |= 1 << slot;
    }
}

#[cfg(test)]
mod test {
    use std::{
        task::{Context, Waker},
        time::Instant,
    };

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
        };

        let mut timer2 = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 27,
        };

        let mut timer3 = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
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
        };

        let mut timer2_copy = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 27,
        };

        let mut timer3_copy = TimerState {
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
            waker: Some(cx.local_waker().clone()),
            deadline: 63,
        };

        unsafe { timer_wheel.add_timer(&raw mut timer1_copy) };
        unsafe { timer_wheel.add_timer(&raw mut timer2_copy) };
        unsafe { timer_wheel.add_timer(&raw mut timer3_copy) };
    }
}
