//! Trusted specifications of the `core` and `alloc` functions which the verified code calls and
//! which `creusot_std` doesn't specify.

use alloc::{alloc::Allocator, vec::Vec};
use core::{
    cmp::Ordering,
    marker::Destruct,
    ops::{Not, Range},
};

use creusot_std::{logic::ord::PartialOrdLogic, prelude::*};
use indexmap::IndexSet;

use crate::verification::Model;

extern_spec! {
    impl<T> Option<T> {
        #[requires(match self { None => true, Some(t) => f.precondition((t,)) })]
        #[ensures(match self {
            None => resolve(f) && result,
            Some(t) => f.postcondition_once((t,), result),
        })]
        fn is_none_or(self, f: impl FnOnce(T) -> bool + Destruct) -> bool;
    }

    impl<Idx: PartialOrd<Idx> + DeepModel<DeepModelTy: PartialOrdLogic>> Range<Idx> {
        #[check(terminates)]
        #[ensures(result == !(self.start.deep_model() < self.end.deep_model()))]
        fn is_empty(&self) -> bool;
    }

    impl Ordering {
        #[check(terminates)]
        #[ensures(result == match self {
            Ordering::Equal => other,
            _ => self,
        })]
        fn then(self, other: Ordering) -> Ordering;
    }

    impl Not for &bool {
        #[check(terminates)]
        #[ensures(result == !*self)]
        fn not(self) -> bool;
    }

    impl usize {
        #[check(terminates)]
        #[requires(self@ + rhs@ <= usize::MAX@)]
        #[ensures(result@ == self@ + rhs@)]
        fn strict_add(self, rhs: usize) -> usize;
    }

    impl<T> [T] {
        #[check(terminates)]
        #[ensures((^self)@ == self@.reverse())]
        fn reverse(&mut self);
    }

    impl<T, A: Allocator + Clone> Vec<T, A> {
        #[check(terminates)]
        #[requires(at@ <= self@.len())]
        #[ensures((^self)@ == self@.subsequence(0, at@))]
        #[ensures(result@ == self@.subsequence(at@, self@.len()))]
        fn split_off(&mut self, at: usize) -> Vec<T, A>;
    }

    impl<T, A: Allocator> Vec<T, A> {
        #[check(terminates)]
        #[ensures((^self)@ == if len@ < self@.len() { self@.subsequence(0, len@) } else { self@ })]
        fn truncate(&mut self, len: usize);

        #[check(terminates)]
        #[requires(self@.len() + other@.len() <= usize::MAX@)]
        #[ensures((^self)@ == self@.concat(other@))]
        #[ensures((^other)@ == Seq::empty())]
        fn append(&mut self, other: &mut Vec<T, A>);
    }

    impl<T: DeepModel, S: Default> Default for IndexSet<T, S> {
        #[check(terminates)]
        #[ensures(result.model() == Seq::empty())]
        fn default() -> Self;
    }
}

/// Rust never allocates more than `isize::MAX` bytes, so a vector of bytes holds at most
/// `isize::MAX` of them.
#[trusted]
#[logic]
#[ensures(v@.len() <= isize::MAX@)]
pub fn lemma_byte_vec_len<A: Allocator>(v: Vec<u8, A>) {}
