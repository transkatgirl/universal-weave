use std::collections::{HashMap, HashSet};

use rkyv::hash::FxHasher64;

#[cfg(creusot)]
use creusot_contracts::prelude::*;

#[cfg(not(creusot))]
use rkyv::{Archive, Deserialize, Serialize};

use crate::Node;

#[cfg_attr(not(creusot), derive(Archive, Deserialize, Serialize))]
#[derive(Debug)]
pub struct DependentNode<T> {
    pub id: u128,
    pub from: Option<u128>,
    pub to: HashSet<u128, FxHasher64>,

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
    nodes: HashMap<u128, DependentNode<T>, FxHasher64>,
    roots: HashSet<u128, FxHasher64>,
    active: Option<u128>,
    bookmarked: HashSet<u128, FxHasher64>,

    pub metadata: M,
}

/*#[requires(a@ < i64::MAX@)]
#[ensures(result@ == a@ + 1)]
pub fn add_one(a: i64) -> i64 {
    a + 1
}*/
