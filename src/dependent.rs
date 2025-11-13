use std::collections::{HashMap, HashSet};

use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

#[cfg(creusot)]
use creusot_contracts::prelude::*;

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
