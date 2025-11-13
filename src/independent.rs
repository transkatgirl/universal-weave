use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use rkyv::hash::FxHasher64;

#[cfg(creusot)]
use creusot_contracts::prelude::*;

#[cfg(not(creusot))]
use rkyv::{Archive, Deserialize, Serialize};

use crate::{IndependentContents, Node};

#[cfg_attr(not(creusot), derive(Archive, Deserialize, Serialize))]
#[derive(Debug)]
pub struct IndependentNode<T>
where
    T: IndependentContents,
{
    pub id: u128,
    pub from: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    pub to: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub active: bool,
    pub bookmarked: bool,
    pub contents: T,
}

impl<T: IndependentContents> Node<T> for IndependentNode<T> {
    fn id(&self) -> u128 {
        self.id
    }
    fn from(&self) -> impl Iterator<Item = u128> {
        self.from.iter().copied()
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
pub struct IndependentWeave<T, M>
where
    T: IndependentContents,
{
    nodes: HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    active: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    bookmarked: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T: IndependentContents, M> IndependentWeave<T, M> {
    #[cfg(not(creusot))]
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            bookmarked: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            metadata,
        }
    }
    #[cfg(not(creusot))]
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots.reserve(additional);
        self.active.reserve(additional);
        self.bookmarked.reserve(additional);
    }
    #[cfg(not(creusot))]
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
}
