//! Trusted specifications of indexmap's [`IndexSet`].
//!
//! A set is modeled as the sequence of the [`Id`]s of its elements, in order, without duplicates.
//! This assumes that elements' [`Hash`] and [`Eq`] implementations agree with their
//! [`DeepModel`], and that hashers built by the same [`BuildHasher`] agree with each other.

use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
    ops::Index,
};

use creusot_std::{logic::Mapping, prelude::*};
use indexmap::{Equivalent, IndexSet};

use crate::verification::{
    Id, Model, compares_by, dedup, id_of, ids, in_order, no_dup, same_elements, sorted_stably,
    total_preorder,
};

impl<T: DeepModel, S> Model for IndexSet<T, S> {
    type ModelTy = Seq<Id<T>>;

    #[trusted]
    #[logic(opaque)]
    #[ensures(no_dup(result))]
    #[ensures(result.len() <= usize::MAX@)]
    fn model(self) -> Self::ModelTy {
        dead
    }
}

extern_spec! {
    impl<T: DeepModel, S> IndexSet<T, S> {
        #[check(terminates)]
        #[ensures(result.model() == Seq::empty())]
        fn with_hasher(hash_builder: S) -> Self;

        #[check(terminates)]
        #[ensures(result.model() == Seq::empty())]
        fn with_capacity_and_hasher(n: usize, hash_builder: S) -> Self;

        #[check(terminates)]
        fn capacity(&self) -> usize;

        #[check(terminates)]
        #[ensures(result@ == self.model().len())]
        fn len(&self) -> usize;

        #[check(terminates)]
        #[ensures(result == (self.model().len() == 0))]
        fn is_empty(&self) -> bool;

        #[check(terminates)]
        #[ensures((^self).model() == Seq::empty())]
        fn clear(&mut self);

        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn reserve(&mut self, additional: usize);

        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn shrink_to_fit(&mut self);

        #[check(terminates)]
        #[ensures(match result {
            Some(v) => index@ < self.model().len() && v.deep_model() == self.model()[index@],
            None => index@ >= self.model().len(),
        })]
        fn get_index<'a>(&'a self, index: usize) -> Option<&'a T>;

        #[check(terminates)]
        #[ensures(match result {
            Some(v) => 0 < self.model().len() && v.deep_model() == self.model()[0],
            None => self.model().len() == 0,
        })]
        fn first<'a>(&'a self) -> Option<&'a T>;

        #[requires(forall<s: F, x: &T> keep.hist_inv(s) && inv(x) && self.model().contains(x.deep_model())
            ==> s.precondition((x,)))]
        #[ensures(forall<x> (^self).model().contains(x) ==> self.model().contains(x))]
        #[ensures(forall<x> self.model().contains(x) && (^self).model().contains(x)
            ==> exists<s1: F, s2: F, t: &T> keep.hist_inv(s1) && t.deep_model() == x
                && s1.postcondition_mut((t,), s2, true))]
        #[ensures(forall<x> self.model().contains(x) && !(^self).model().contains(x)
            ==> exists<s1: F, s2: F, t: &T> keep.hist_inv(s1) && t.deep_model() == x
                && s1.postcondition_mut((t,), s2, false))]
        #[ensures(in_order((^self).model(), self.model()))]
        fn retain<F>(&mut self, keep: F)
        where
            F: FnMut(&T) -> bool;

        #[requires(forall<s: F> cmp.hist_inv(s)
            ==> forall<a: &T, b: &T> inv(a) && inv(b) ==> s.precondition((a, b)))]
        #[ensures((^self).model().len() == self.model().len())]
        #[ensures(same_elements((^self).model(), self.model()))]
        #[ensures(forall<le: Mapping<Id<T>, Mapping<Id<T>, bool>>>
            total_preorder(le) && compares_by(cmp, id_of::<T>(), le)
            ==> sorted_stably((^self).model(), self.model(), le))]
        fn sort_by<F>(&mut self, cmp: F)
        where
            F: FnMut(&T, &T) -> Ordering;
    }

    impl<T: Hash + Eq + DeepModel, S: BuildHasher> IndexSet<T, S> {
        #[check(terminates)]
        #[ensures(result == !self.model().contains(value.deep_model()))]
        #[ensures(if result {
            (^self).model() == self.model().snoc(value.deep_model())
        } else {
            (^self).model() == self.model()
        })]
        #[ensures(forall<x> (^self).model().contains(x)
            == (self.model().contains(x) || x == value.deep_model()))]
        fn insert(&mut self, value: T) -> bool;

        #[check(terminates)]
        #[requires(if self.model().contains(value.deep_model()) {
            index@ < self.model().len()
        } else {
            index@ <= self.model().len()
        })]
        #[ensures(result == !self.model().contains(value.deep_model()))]
        #[ensures(result ==> (^self).model() == self.model().add_at(index@, value.deep_model()))]
        #[ensures(forall<i> 0 <= i && i < self.model().len() && self.model()[i] == value.deep_model()
            ==> (^self).model() == self.model().removed(i).add_at(index@, value.deep_model()))]
        #[ensures(forall<x> (^self).model().contains(x)
            == (self.model().contains(x) || x == value.deep_model()))]
        fn shift_insert(&mut self, index: usize, value: T) -> bool;

        #[check(terminates)]
        #[requires(index@ < self.model().len())]
        #[ensures(match result {
            Ok(old) => old.deep_model() == self.model()[index@]
                && (^self).model() == self.model().set(index@, value.deep_model())
                && forall<x> (^self).model().contains(x)
                    == ((self.model().contains(x) && x != self.model()[index@]) || x == value.deep_model()),
            Err((other, v)) => other@ < self.model().len()
                && other@ != index@
                && self.model()[other@] == value.deep_model()
                && v == value
                && (^self).model() == self.model(),
        })]
        fn replace_index(&mut self, index: usize, value: T) -> Result<T, (usize, T)>;

        #[check(terminates)]
        #[ensures(result == self.model().contains(value.deep_model()))]
        fn contains<Q>(&self, value: &Q) -> bool
        where
            Q: ?Sized + Hash + Equivalent<T> + DeepModel<DeepModelTy = Id<T>>;

        #[check(terminates)]
        #[ensures(match result {
            Some(i) => i@ < self.model().len() && self.model()[i@] == value.deep_model(),
            None => !self.model().contains(value.deep_model()),
        })]
        fn get_index_of<Q>(&self, value: &Q) -> Option<usize>
        where
            Q: ?Sized + Hash + Equivalent<T> + DeepModel<DeepModelTy = Id<T>>;

        #[check(terminates)]
        #[ensures(result == self.model().contains(value.deep_model()))]
        #[ensures(!result ==> (^self).model() == self.model())]
        #[ensures(forall<i> 0 <= i && i < self.model().len() && self.model()[i] == value.deep_model()
            ==> (^self).model() == self.model().removed(i))]
        #[ensures(forall<x> (^self).model().contains(x)
            == (self.model().contains(x) && x != value.deep_model()))]
        fn shift_remove<Q>(&mut self, value: &Q) -> bool
        where
            Q: ?Sized + Hash + Equivalent<T> + DeepModel<DeepModelTy = Id<T>>;
    }

    impl<T: Hash + Eq + DeepModel, S: BuildHasher> IndexSet<T, S> {
        #[check(terminates)]
        #[ensures(result == forall<x> !(self.model().contains(x) && other.model().contains(x)))]
        fn is_disjoint<S2>(&self, other: &IndexSet<T, S2>) -> bool
        where
            S2: BuildHasher;
    }

    impl<T: Hash + Eq + DeepModel, S: BuildHasher + Default> FromIterator<T> for IndexSet<T, S> {
        #[requires(I::into_iter.precondition((iterable,)))]
        #[ensures(exists<into_iter: I::IntoIter, done: &mut I::IntoIter, items: Seq<T>>
            I::into_iter.postcondition((iterable,), into_iter)
            && into_iter.produces(items, *done) && done.completed() && resolve(^done)
            && result.model() == dedup(ids(items)))]
        fn from_iter<I: IntoIterator<Item = T, IntoIter: IteratorSpec>>(iterable: I) -> Self;
    }

    impl<T: DeepModel, S> Index<usize> for IndexSet<T, S> {
        #[check(terminates)]
        #[requires(index@ < self.model().len())]
        #[ensures(result.deep_model() == self.model()[index@])]
        fn index<'a>(&'a self, index: usize) -> &'a T;
    }
}
