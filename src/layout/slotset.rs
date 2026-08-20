#![allow(clippy::expect_used, reason = "API")]

use scratchpads::{ScratchpadGuard, ScratchpadVec};

pub struct SlotSet<'g> {
    words: ScratchpadVec<'g, u64>,
    levels: ScratchpadVec<'g, usize>,
}

fn low_bit(word: u64) -> usize {
    usize::try_from(word.trailing_zeros()).unwrap()
}

fn high_bit(word: u64) -> usize {
    usize::try_from(word.ilog2()).unwrap()
}

#[allow(clippy::arithmetic_side_effects, reason = "Cannot overflow")]
impl<'g> SlotSet<'g> {
    pub const fn new(guard: &'g ScratchpadGuard<'_>) -> Self {
        Self {
            words: guard.vec(),
            levels: guard.vec(),
        }
    }
    pub fn rebuild(&mut self, len: usize) {
        self.words.clear();
        self.levels.clear();

        let mut total = 0_usize;
        let mut below = len;

        loop {
            let words = ((below >> 6_u32) + usize::from(below & 63 != 0)).max(1);

            self.levels.push(total);
            total += words;

            if words == 1 {
                break;
            }

            below = words;
        }

        self.words.resize(total, 0);
    }
    pub fn is_empty(&self) -> bool {
        let &offset = self.levels.last().expect("Set must be rebuilt before use");

        self.words[offset] == 0
    }
    /*pub fn contains(&self, slot: usize) -> bool {
        self.words[slot >> 6_u32] & (1_u64 << (slot & 63)) != 0
    }*/
    pub fn insert(&mut self, slot: usize) {
        let mut index = slot;

        for &offset in &self.levels {
            let word = &mut self.words[offset + (index >> 6_u32)];
            let was = *word;

            *word = was | (1_u64 << (index & 63));

            if was != 0 {
                break;
            }

            index >>= 6_u32;
        }
    }
    pub fn remove(&mut self, slot: usize) {
        let mut index = slot;

        for &offset in &self.levels {
            let word = &mut self.words[offset + (index >> 6_u32)];

            *word &= !(1_u64 << (index & 63));

            if *word != 0 {
                break;
            }

            index >>= 6_u32;
        }
    }
    pub fn predecessor(&self, slot: usize) -> Option<usize> {
        let mut index = slot;
        let mut level = 0_usize;

        loop {
            let &offset = self.levels.get(level)?;
            let word_index = index >> 6_u32;
            let masked = self.words[offset + word_index] & !(u64::MAX << (index & 63));

            if masked != 0 {
                index = (word_index << 6_u32) | high_bit(masked);

                break;
            }

            index = word_index;
            level += 1;
        }

        while level > 0 {
            level -= 1;

            let offset = self.levels[level];

            index = (index << 6_u32) | high_bit(self.words[offset + index]);
        }

        Some(index)
    }
    pub fn successor(&self, slot: usize) -> Option<usize> {
        let mut index = slot;
        let mut level = 0_usize;

        loop {
            let &offset = self.levels.get(level)?;
            let word_index = index >> 6_u32;
            let masked = self.words[offset + word_index] & ((u64::MAX << (index & 63)) << 1_u32);

            if masked != 0 {
                index = (word_index << 6_u32) | low_bit(masked);

                break;
            }

            index = word_index;
            level += 1;
        }

        while level > 0 {
            level -= 1;

            let offset = self.levels[level];

            index = (index << 6_u32) | low_bit(self.words[offset + index]);
        }

        Some(index)
    }
    pub fn first(&self) -> Option<usize> {
        self.extreme(low_bit)
    }
    pub fn last(&self) -> Option<usize> {
        self.extreme(high_bit)
    }
    fn extreme(&self, bit: impl Fn(u64) -> usize) -> Option<usize> {
        let &offset = self.levels.last().expect("Set must be rebuilt before use");
        let top = self.words[offset];

        if top == 0 {
            return None;
        }

        let mut level = self.levels.len() - 1;
        let mut index = bit(top);

        while level > 0 {
            level -= 1;

            let offset = self.levels[level];

            index = (index << 6_u32) | bit(self.words[offset + index]);
        }

        Some(index)
    }
}
