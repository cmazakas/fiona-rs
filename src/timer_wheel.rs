// Copyright 2026 Christian Mazakas
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// This code is a shameless ripoff of Tokio's approach, who we're using as our
// reference implementation. The Tokio authors deserve all credit for their
// genius in this implementation.

#![allow(clippy::struct_field_names)]

use std::task::LocalWaker;

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
    unsafe fn add_timer(&mut self, timer: *mut TimerState) {
        let timer = unsafe { &mut *timer };
        assert!(timer.deadline > self.elapsed);

        let level = level_for(self.elapsed, timer.deadline);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // Shamelessly copy Tokio's test from here:
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
}
