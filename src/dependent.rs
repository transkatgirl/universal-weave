use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use rkyv::hash::FxHasher64;

#[cfg(creusot)]
use creusot_contracts::prelude::*;

#[cfg(not(creusot))]
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    DiscreteContents, DiscreteWeave, DuplicatableContents, DuplicatableWeave, Node, Weave,
};

#[cfg_attr(not(creusot), derive(Archive, Deserialize, Serialize))]
#[derive(Debug)]
pub struct DependentNode<T> {
    pub id: u128,
    pub from: Option<u128>,
    pub to: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub active: bool,
    pub bookmarked: bool,
    pub contents: T,
}

impl<T> Node<T> for DependentNode<T> {
    fn id(&self) -> u128 {
        self.id
    }
    fn from(&self) -> impl Iterator<Item = u128> {
        self.from.into_iter()
    }
    fn to(&self) -> impl Iterator<Item = u128> {
        self.to.iter().copied()
    }
    fn is_active(&self) -> bool {
        self.active
    }
    fn is_bookmarked(&self) -> bool {
        self.bookmarked
    }
    fn contents(&self) -> &T {
        &self.contents
    }
}

#[cfg_attr(not(creusot), derive(Archive, Deserialize, Serialize))]
#[derive(Debug)]
pub struct DependentWeave<T, M> {
    nodes: HashMap<u128, DependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    active: Option<u128>,
    bookmarked: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T, M> DependentWeave<T, M> {
    #[cfg(not(creusot))]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: None,
            bookmarked: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            metadata,
        }
    }
    #[cfg(not(creusot))]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots.reserve(additional);
        self.bookmarked.reserve(additional);
    }
    #[cfg(not(creusot))]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
    #[cfg(not(creusot))]
    pub fn get_active_thread(&self) -> Option<u128> {
        self.active
    }
}

impl<T, M> Weave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn get_node(&self, id: u128) -> Option<&DependentNode<T>> {
        self.nodes.get(&id)
    }

    fn get_roots(&self) -> impl Iterator<Item = u128> {
        self.roots.iter().copied()
    }

    fn get_bookmarks(&self) -> impl Iterator<Item = u128> {
        self.bookmarked.iter().copied()
    }

    fn get_active_threads(&self) -> impl Iterator<Item = u128> {
        self.active.into_iter()
    }

    fn add_node(&mut self, node: DependentNode<T>) -> bool {
        todo!()
    }

    fn set_node_active_status(&mut self, id: u128, value: bool) -> bool {
        todo!()
    }

    fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool {
        todo!()
    }

    fn remove_node(&mut self, id: u128) -> Option<DependentNode<T>> {
        todo!()
    }
}

impl<T: DiscreteContents, M> DiscreteWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn split_node(&mut self, id: u128, at: usize) -> bool {
        todo!()
    }

    fn merge_with_parent(&mut self, id: u128) -> bool {
        todo!()
    }
}

/*impl<T: DuplicatableContents, M> DuplicatableWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn find_duplicates(&self, id: u128) -> impl Iterator<Item = u128> {
        todo!()
    }
}*/

/*#[requires(a@ < i64::MAX@)]
#[ensures(result@ == a@ + 1)]
pub fn add_one(a: i64) -> i64 {
    a + 1
}*/
