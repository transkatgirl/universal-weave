use std::collections::{HashMap, HashSet};

use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

#[cfg(creusot)]
use creusot_contracts::prelude::*;

use crate::IndependentContents;

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
