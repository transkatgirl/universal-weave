use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use creusot_contracts::{
    logic::{FMap, FSet},
    model::View,
};
use rkyv::hash::FxHasher64;

//#[cfg(creusot)]
use creusot_contracts::prelude::*;

#[cfg(not(creusot))]
use rkyv::{Archive, Deserialize, Serialize};

use crate::{
    DiscreteContents, DiscreteWeave, DuplicatableContents, DuplicatableWeave, IndependentContents,
    Node, Weave,
};

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

#[cfg(creusot)]
pub struct IndependentNodeView<T>
where
    T: IndependentContents,
{
    id: u128,
    from: FSet<u128>,
    to: FSet<u128>,

    active: bool,
    bookmarked: bool,
    contents: T,
}

#[cfg(creusot)]
impl<T> View for IndependentNode<T>
where
    T: IndependentContents,
{
    type ViewTy = IndependentNodeView<T>;

    #[logic(opaque)]
    fn view(self) -> Self::ViewTy {
        dead
    }
}

#[cfg(creusot)]
impl<T> Invariant for IndependentNode<T>
where
    T: IndependentContents,
{
    #[logic]
    fn invariant(self) -> bool {
        pearlite! {
            self@.from.intersection(self@.to) == FSet::empty() && !self@.from.contains(self@.id) && !self@.to.contains(self@.id)
        }
    }
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

#[cfg(creusot)]
pub struct IndependentWeaveView<T>
where
    T: IndependentContents,
{
    nodes: FMap<u128, IndependentNode<T>>,
    roots: FSet<u128>,
    active: FSet<u128>,
    bookmarked: FSet<u128>,
}

#[cfg(creusot)]
impl<T, M> View for IndependentWeave<T, M>
where
    T: IndependentContents,
{
    type ViewTy = IndependentWeaveView<T>;

    #[logic(opaque)]
    fn view(self) -> Self::ViewTy {
        dead
    }
}

//#[cfg(creusot)]
/*impl<T, M> Invariant for IndependentWeave<T, M>
where
    T: IndependentContents,
{
    #[logic]
    fn invariant(self) -> bool {
        pearlite! {
            true
        }
    }
}*/

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
    #[requires(self@.nodes.contains(id))]
    fn siblings(&self, id: u128) -> impl Iterator<Item = &IndependentNode<T>> {
        self.nodes.get(&id).into_iter().flat_map(|node| {
            node.from.iter().copied().flat_map(|id| {
                self.nodes
                    .get(&id)
                    .into_iter()
                    .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
                    .filter_map(|id| self.nodes.get(&id))
            })
        })
    }
}

impl<T: IndependentContents, M> Weave<IndependentNode<T>, T> for IndependentWeave<T, M> {
    fn get_node(&self, id: u128) -> Option<&IndependentNode<T>> {
        self.nodes.get(&id)
    }

    fn get_roots(&self) -> impl Iterator<Item = u128> {
        self.roots.iter().copied()
    }

    fn get_bookmarks(&self) -> impl Iterator<Item = u128> {
        self.bookmarked.iter().copied()
    }

    fn get_active_threads(&self) -> impl Iterator<Item = u128> {
        self.active.iter().copied()
    }

    fn add_node(&mut self, node: IndependentNode<T>) -> bool {
        todo!()
    }

    fn set_node_active_status(&mut self, id: u128, value: bool) -> bool {
        todo!()
    }

    fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool {
        todo!()
    }

    fn remove_node(&mut self, id: u128) -> Option<IndependentNode<T>> {
        todo!()
    }
}

impl<T: DiscreteContents + IndependentContents, M> DiscreteWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    fn split_node(&mut self, id: u128, at: usize) -> bool {
        todo!()
    }

    fn merge_with_parent(&mut self, id: u128) -> bool {
        todo!()
    }
}

/*impl<T: DuplicatableContents + IndependentContents, M> DuplicatableWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    fn find_duplicates(&self, id: u128) -> impl Iterator<Item = u128> {
        todo!()
    }
}*/

impl<T: IndependentContents, M> crate::IndependentWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    fn replace_node_parents(&mut self, target: u128, parents: &[u128]) -> bool {
        todo!()
    }
}
