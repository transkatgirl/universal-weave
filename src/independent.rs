use std::collections::{HashMap, HashSet};

use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

use crate::IndependentContents;

#[derive(Archive, Deserialize, Serialize, Debug)]
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

#[derive(Archive, Deserialize, Serialize, Debug)]
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
