use std::collections::{HashMap, HashSet};

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
    pub from: HashSet<u128, FxHasher64>,
    pub to: HashSet<u128, FxHasher64>,

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
    nodes: HashMap<u128, IndependentNode<T>, FxHasher64>,
    roots: HashSet<u128, FxHasher64>,
    active: HashSet<u128, FxHasher64>,
    bookmarked: HashSet<u128, FxHasher64>,

    pub metadata: M,
}
