//! Trusted specifications of hashbrown's [`HashMap`] and [`HashSet`], which also back
//! `ScratchpadMap` and `ScratchpadSet`.
//!
//! A map is modeled as a finite map from the [`Id`] of each key to its value, and a set as a finite
//! set of [`Id`]s. This assumes that keys' [`Hash`] and [`Eq`] implementations agree with their
//! [`DeepModel`], and that hashers built by the same [`BuildHasher`] agree with each other.

use core::{
    hash::{BuildHasher, Hash},
    ops::Index,
};

use creusot_std::{
    logic::{FMap, FSet},
    prelude::*,
};
use hashbrown::{
    Equivalent, HashMap, HashSet,
    hash_map::{Entry, OccupiedEntry, VacantEntry},
};
use scratchpads::allocator_api2::alloc::{Allocator, Global};

use crate::verification::{Id, Model};

impl<K: DeepModel, V, S, A: Allocator> Model for HashMap<K, V, S, A> {
    type ModelTy = FMap<Id<K>, V>;

    #[trusted]
    #[logic(opaque)]
    #[ensures(result.len() <= usize::MAX@)]
    fn model(self) -> Self::ModelTy {
        dead
    }
}

impl<T: DeepModel, S, A: Allocator> Model for HashSet<T, S, A> {
    type ModelTy = FSet<Id<T>>;

    #[trusted]
    #[logic(opaque)]
    #[ensures(result.len() <= usize::MAX@)]
    fn model(self) -> Self::ModelTy {
        dead
    }
}

/// The logical model of hashbrown's map entries.
///
/// An entry mutably borrows its map: [`EntryModel::current`] is the map's contents when the entry
/// was created, and [`EntryModel::fin`] its contents once the entry is gone.
pub trait EntryModel {
    /// The model of keys.
    type Key;
    /// The type of values.
    type Value;

    /// The model of the entry's key.
    #[logic]
    fn key_model(self) -> Self::Key;

    /// The contents of the map when the entry was created.
    #[logic]
    fn current(self) -> FMap<Self::Key, Self::Value>;

    /// The contents of the map once the entry is gone.
    #[logic(prophetic)]
    fn fin(self) -> FMap<Self::Key, Self::Value>;
}

impl<K: DeepModel, V, S, A: Allocator> EntryModel for OccupiedEntry<'_, K, V, S, A> {
    type Key = Id<K>;
    type Value = V;

    #[logic(opaque)]
    fn key_model(self) -> Id<K> {
        dead
    }

    #[logic(opaque)]
    fn current(self) -> FMap<Id<K>, V> {
        dead
    }

    #[logic(opaque, prophetic)]
    fn fin(self) -> FMap<Id<K>, V> {
        dead
    }
}

impl<K: DeepModel, V, S, A: Allocator> EntryModel for VacantEntry<'_, K, V, S, A> {
    type Key = Id<K>;
    type Value = V;

    #[logic(opaque)]
    fn key_model(self) -> Id<K> {
        dead
    }

    #[logic(opaque)]
    fn current(self) -> FMap<Id<K>, V> {
        dead
    }

    #[logic(opaque, prophetic)]
    fn fin(self) -> FMap<Id<K>, V> {
        dead
    }
}

extern_spec! {
    impl<K: DeepModel, V, S> HashMap<K, V, S> {
        #[check(terminates)]
        #[ensures(result.model() == FMap::empty())]
        fn with_hasher(hash_builder: S) -> Self;

        #[check(terminates)]
        #[ensures(result.model() == FMap::empty())]
        fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self;
    }

    impl<K: DeepModel, V, S, A: Allocator> HashMap<K, V, S, A> {
        #[check(terminates)]
        fn capacity(&self) -> usize;

        #[check(terminates)]
        #[ensures(result@ == self.model().len())]
        fn len(&self) -> usize;

        #[check(terminates)]
        #[ensures(result == (self.model().len() == 0))]
        fn is_empty(&self) -> bool;

        #[check(terminates)]
        #[ensures((^self).model() == FMap::empty())]
        fn clear(&mut self);
    }

    impl<K: Eq + Hash + DeepModel, V, S: BuildHasher, A: Allocator> HashMap<K, V, S, A> {
        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn reserve(&mut self, additional: usize);

        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn shrink_to_fit(&mut self);

        #[check(terminates)]
        #[ensures(result == self.model().contains(&k.deep_model()))]
        fn contains_key<Q>(&self, k: &Q) -> bool
        where
            Q: Hash + Equivalent<K> + ?Sized + DeepModel<DeepModelTy = Id<K>>;

        #[check(terminates)]
        #[ensures(match result {
            Some(v) => self.model().lookup(k.deep_model()) == Some(*v),
            None => !self.model().contains(&k.deep_model()),
        })]
        fn get<'a, Q>(&'a self, k: &Q) -> Option<&'a V>
        where
            Q: Hash + Equivalent<K> + ?Sized + DeepModel<DeepModelTy = Id<K>>;

        #[check(terminates)]
        #[ensures(match result {
            Some(v) => self.model().lookup(k.deep_model()) == Some(*v)
                && (^self).model() == self.model().add(k.deep_model(), ^v),
            None => !self.model().contains(&k.deep_model()) && (^self).model() == self.model(),
        })]
        fn get_mut<'a, Q>(&'a mut self, k: &Q) -> Option<&'a mut V>
        where
            Q: Hash + Equivalent<K> + ?Sized + DeepModel<DeepModelTy = Id<K>>;

        #[check(terminates)]
        #[ensures((^self).model() == self.model().add(k.deep_model(), v))]
        #[ensures(result == self.model().lookup(k.deep_model()))]
        fn insert(&mut self, k: K, v: V) -> Option<V>;

        #[check(terminates)]
        #[ensures((^self).model() == self.model().remove(k.deep_model()))]
        #[ensures(result == self.model().lookup(k.deep_model()))]
        fn remove<Q>(&mut self, k: &Q) -> Option<V>
        where
            Q: Hash + Equivalent<K> + ?Sized + DeepModel<DeepModelTy = Id<K>>;

        #[check(terminates)]
        #[ensures(match result {
            Entry::Occupied(e) => self.model().contains(&key.deep_model())
                && e.key_model() == key.deep_model()
                && e.current() == self.model()
                && e.fin() == (^self).model()
                && (resolve(e) ==> e.fin() == e.current()),
            Entry::Vacant(e) => !self.model().contains(&key.deep_model())
                && e.key_model() == key.deep_model()
                && e.current() == self.model()
                && e.fin() == (^self).model()
                && (resolve(e) ==> e.fin() == e.current()),
        })]
        fn entry<'a>(&'a mut self, key: K) -> Entry<'a, K, V, S, A>;
    }

    impl<'q, K, Q, V, S, A> Index<&'q Q> for HashMap<K, V, S, A>
    where
        K: Eq + Hash + DeepModel,
        Q: Hash + Equivalent<K> + ?Sized + DeepModel<DeepModelTy = Id<K>>,
        S: BuildHasher,
        A: Allocator,
    {
        #[check(terminates)]
        #[requires(self.model().contains(&key.deep_model()))]
        #[ensures(self.model().lookup(key.deep_model()) == Some(*result))]
        fn index<'a>(&'a self, key: &'q Q) -> &'a V;
    }

    impl<'a, K: DeepModel, V, S, A: Allocator> OccupiedEntry<'a, K, V, S, A> {
        #[check(terminates)]
        #[ensures(self.current().lookup(self.key_model()) == Some(*result))]
        fn get<'b>(&'b self) -> &'b V;

        #[check(terminates)]
        #[ensures(self.current().lookup(self.key_model()) == Some(*result))]
        #[ensures(self.fin() == self.current().add(self.key_model(), ^result))]
        fn into_mut(self) -> &'a mut V;
    }

    impl<'a, K: DeepModel + Hash, V, S: BuildHasher, A: Allocator> VacantEntry<'a, K, V, S, A> {
        #[check(terminates)]
        #[ensures(*result == value)]
        #[ensures(self.fin() == self.current().add(self.key_model(), ^result))]
        fn insert(self, value: V) -> &'a mut V;

        #[check(terminates)]
        #[ensures(result.key_model() == self.key_model())]
        #[ensures(result.current() == self.current().add(self.key_model(), value))]
        #[ensures(result.fin() == self.fin())]
        #[ensures(resolve(result) ==> result.fin() == result.current())]
        fn insert_entry(self, value: V) -> OccupiedEntry<'a, K, V, S, A>;
    }

    impl<'a, K: DeepModel + Hash, V, S: BuildHasher, A: Allocator> Entry<'a, K, V, S, A> {
        #[requires(match self {
            Entry::Occupied(_) => true,
            Entry::Vacant(_) => default.precondition(()),
        })]
        #[ensures(match self {
            Entry::Occupied(e) => e.current().lookup(e.key_model()) == Some(*result)
                && e.fin() == e.current().add(e.key_model(), ^result)
                && resolve(default),
            Entry::Vacant(e) => default.postcondition_once((), *result)
                && e.fin() == e.current().add(e.key_model(), ^result),
        })]
        fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V;
    }

    impl<T: DeepModel, S> HashSet<T, S, Global> {
        #[check(terminates)]
        #[ensures(result.model() == FSet::empty())]
        fn with_hasher(hasher: S) -> Self;

        #[check(terminates)]
        #[ensures(result.model() == FSet::empty())]
        fn with_capacity_and_hasher(capacity: usize, hasher: S) -> Self;
    }

    impl<T: DeepModel, S, A: Allocator> HashSet<T, S, A> {
        #[check(terminates)]
        fn capacity(&self) -> usize;

        #[check(terminates)]
        #[ensures(result@ == self.model().len())]
        fn len(&self) -> usize;

        #[check(terminates)]
        #[ensures(result == (self.model().len() == 0))]
        fn is_empty(&self) -> bool;

        #[check(terminates)]
        #[ensures((^self).model() == FSet::empty())]
        fn clear(&mut self);
    }

    impl<T: Eq + Hash + DeepModel, S: BuildHasher, A: Allocator> HashSet<T, S, A> {
        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn reserve(&mut self, additional: usize);

        #[check(terminates)]
        #[ensures((^self).model() == self.model())]
        fn shrink_to_fit(&mut self);

        #[check(terminates)]
        #[ensures(result == self.model().contains(&value.deep_model()))]
        fn contains<Q>(&self, value: &Q) -> bool
        where
            Q: Hash + Equivalent<T> + ?Sized + DeepModel<DeepModelTy = Id<T>>;

        #[check(terminates)]
        #[ensures((^self).model() == self.model().add(value.deep_model()))]
        #[ensures(result == !self.model().contains(&value.deep_model()))]
        fn insert(&mut self, value: T) -> bool;

        #[check(terminates)]
        #[ensures((^self).model() == self.model().remove(value.deep_model()))]
        #[ensures(result == self.model().contains(&value.deep_model()))]
        fn remove<Q>(&mut self, value: &Q) -> bool
        where
            Q: Hash + Equivalent<T> + ?Sized + DeepModel<DeepModelTy = Id<T>>;
    }
}
