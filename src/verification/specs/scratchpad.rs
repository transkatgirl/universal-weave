//! Trusted specifications of scratchpads' [`Scratchpad`] and [`ScratchpadGuard`], and of
//! allocator-api2's [`Vec`], which backs `ScratchpadVec`.
//!
//! `ScratchpadMap` and `ScratchpadSet` are hashbrown collections, specified in
//! [`hash_map`](super::hash_map).

use core::{
    hash::BuildHasher,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use creusot_std::{
    logic::{FMap, FSet},
    prelude::*,
    std::slice::SliceIndexSpec,
};
use scratchpads::{
    Scratchpad, ScratchpadGuard, ScratchpadMap, ScratchpadSet, ScratchpadVec,
    allocator_api2::{alloc::Allocator, vec::Vec},
};

use crate::verification::Model;

impl<T, A: Allocator> Model for Vec<T, A> {
    type ModelTy = Seq<T>;

    #[trusted]
    #[logic(opaque)]
    #[ensures(result.len() <= usize::MAX@)]
    fn model(self) -> Self::ModelTy {
        dead
    }
}

extern_spec! {
    impl Scratchpad {
        #[check(terminates)]
        fn new() -> Scratchpad;

        #[check(terminates)]
        fn guard<'a>(&'a mut self) -> ScratchpadGuard<'a>;
    }

    impl<'a> ScratchpadGuard<'a> {
        #[check(terminates)]
        #[ensures(result.model() == Seq::empty())]
        fn vec<'b, T>(&'b self) -> ScratchpadVec<'b, T>;

        #[check(terminates)]
        #[ensures(result.model() == Seq::empty())]
        fn vec_with_capacity<'b, T>(&'b self, capacity: usize) -> ScratchpadVec<'b, T>;

        #[check(terminates)]
        #[ensures(result.model() == FSet::empty())]
        fn set<'b, T: DeepModel, S: BuildHasher>(&'b self, hasher: S) -> ScratchpadSet<'b, T, S>;

        #[check(terminates)]
        #[ensures(result.model() == FSet::empty())]
        fn set_with_capacity<'b, T: DeepModel, S: BuildHasher>(
            &'b self,
            capacity: usize,
            hasher: S,
        ) -> ScratchpadSet<'b, T, S>;

        #[check(terminates)]
        #[ensures(result.model() == FMap::empty())]
        fn map<'b, K: DeepModel, V, S: BuildHasher>(&'b self, hasher: S) -> ScratchpadMap<'b, K, V, S>;

        #[check(terminates)]
        #[ensures(result.model() == FMap::empty())]
        fn map_with_capacity<'b, K: DeepModel, V, S: BuildHasher>(
            &'b self,
            capacity: usize,
            hasher: S,
        ) -> ScratchpadMap<'b, K, V, S>;
    }

    impl<T, A: Allocator> Vec<T, A> {
        #[check(terminates)]
        #[ensures(result@ == self.model().len())]
        fn len(&self) -> usize;

        #[check(terminates)]
        #[ensures(result == (self.model().len() == 0))]
        fn is_empty(&self) -> bool;

        #[check(terminates)]
        #[ensures((^self).model() == self.model().snoc(value))]
        fn push(&mut self, value: T);

        #[check(terminates)]
        #[ensures(match result {
            Some(t) => (^self).model() == self.model().subsequence(0, self.model().len() - 1)
                && self.model() == (^self).model().snoc(t),
            None => (^self).model() == self.model() && self.model().len() == 0,
        })]
        fn pop(&mut self) -> Option<T>;

        #[check(terminates)]
        #[ensures((^self).model() == Seq::empty())]
        fn clear(&mut self);

        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn reserve(&mut self, additional: usize);
    }

    impl<T: Clone, A: Allocator> Vec<T, A> {
        #[check(terminates)]
        #[ensures((^self).model().len() == self.model().len() + other@.len())]
        #[ensures(forall<i> 0 <= i && i < self.model().len() ==> (^self).model()[i] == self.model()[i])]
        #[ensures(forall<i> 0 <= i && i < other@.len()
            ==> T::clone.postcondition((&other@[i],), (^self).model()[self.model().len() + i]))]
        fn extend_from_slice(&mut self, other: &[T]);
    }

    impl<T, A: Allocator> Deref for Vec<T, A> {
        #[check(terminates)]
        #[ensures(result@ == self.model())]
        fn deref<'a>(&'a self) -> &'a [T];
    }

    impl<T, A: Allocator> DerefMut for Vec<T, A> {
        #[check(terminates)]
        #[ensures(result@ == self.model())]
        #[ensures((^result)@ == (^self).model())]
        fn deref_mut<'a>(&'a mut self) -> &'a mut [T];
    }

    impl<T, I: SliceIndexSpec<[T]>, A: Allocator> Index<I> for Vec<T, A> {
        #[check(terminates)]
        #[requires(ix.in_bounds(self.model()))]
        #[ensures(ix.has_value(self.model(), *result))]
        fn index<'a>(&'a self, ix: I) -> &'a <Vec<T, A> as Index<I>>::Output;
    }

    impl<T, I: SliceIndexSpec<[T]>, A: Allocator> IndexMut<I> for Vec<T, A> {
        #[check(terminates)]
        #[requires(ix.in_bounds(self.model()))]
        #[ensures(ix.has_value(self.model(), *result))]
        #[ensures(ix.has_value((^self).model(), ^result))]
        #[ensures(ix.resolve_elswhere(self.model(), (^self).model()))]
        #[ensures((^self).model().len() == self.model().len())]
        fn index_mut<'a>(&'a mut self, ix: I) -> &'a mut <Vec<T, A> as Index<I>>::Output;
    }
}
