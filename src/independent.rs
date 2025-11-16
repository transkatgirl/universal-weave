use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use contracts::*;
use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

use crate::{
    DiscreteContents, DiscreteWeave, DuplicatableContents, DuplicatableWeave, IndependentContents,
    Node, Weave,
};

#[derive(Archive, Deserialize, Serialize, Debug)]
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

impl<T> IndependentNode<T>
where
    T: IndependentContents,
{
    fn verify(&self) -> bool {
        self.from.is_disjoint(&self.to)
            && !self.from.contains(&self.id)
            && !self.to.contains(&self.id)
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

#[derive(Archive, Deserialize, Serialize, Debug)]
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

impl<T, M> IndependentWeave<T, M>
where
    T: IndependentContents,
{
    fn verify(&self) -> bool {
        let nodes: HashSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();

        self.roots.is_subset(&nodes)
            && self.active.is_subset(&nodes)
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.verify()
                    && value.id == *key
                    && value.from.is_subset(&nodes)
                    && value.to.is_subset(&nodes)
                    && value.from.is_empty() == self.roots.contains(key)
                    && value.active == self.active.contains(key)
                    && value.bookmarked == self.bookmarked.contains(key)
                    && value
                        .from
                        .iter()
                        .map(|v| self.nodes.get(v).unwrap())
                        .all(|p| p.to.contains(key))
                    && value
                        .to
                        .iter()
                        .map(|v| self.nodes.get(v).unwrap())
                        .all(|p| p.from.contains(key))
                    && if value.active && !value.from.is_empty() {
                        !value.from.is_disjoint(&self.active)
                    } else {
                        true
                    }
            })
    }
    fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
}

impl<T: IndependentContents, M> IndependentWeave<T, M> {
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            bookmarked: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            metadata,
        }
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots.reserve(additional);
        self.active.reserve(additional);
        self.bookmarked.reserve(additional);
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
    fn siblings(&self, node: &IndependentNode<T>) -> impl Iterator<Item = &IndependentNode<T>> {
        node.from.iter().copied().flat_map(|id| {
            self.nodes
                .get(&id)
                .into_iter()
                .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
                .filter_map(|id| self.nodes.get(&id))
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

    //#[debug_ensures(self.verify())]
    //#[ensures(self.under_max_size())]
    fn add_node(&mut self, node: IndependentNode<T>) -> bool {
        todo!()
    }

    //#[debug_ensures(value == (self.active == Some(id)))]
    //#[debug_ensures(self.verify())]
    fn set_node_active_status(&mut self, id: u128, value: bool) -> bool {
        todo!()
    }

    #[debug_ensures(value == self.bookmarked.contains(&id))]
    #[debug_ensures(self.verify())]
    fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool {
        match self.nodes.get_mut(&id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(id);
                } else {
                    self.bookmarked.remove(&id);
                }

                true
            }
            None => false,
        }
    }

    //#[debug_ensures(!self.nodes.contains_key(&id))]
    //#[debug_ensures(self.verify())]
    fn remove_node(&mut self, id: u128) -> Option<IndependentNode<T>> {
        todo!()
    }
}

impl<T: DiscreteContents + IndependentContents, M> DiscreteWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    //#[debug_ensures(self.verify())]
    //#[ensures(self.under_max_size())]
    fn split_node(&mut self, id: u128, at: usize, new_id: u128) -> bool {
        todo!()
    }

    //#[debug_ensures(self.verify())]
    fn merge_with_parent(&mut self, id: u128) -> bool {
        todo!()
    }
}

impl<T: DuplicatableContents + IndependentContents, M> DuplicatableWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    fn find_duplicates(&self, id: u128) -> impl Iterator<Item = u128> {
        self.nodes.get(&id).into_iter().flat_map(|node| {
            self.siblings(node).filter_map(|sibling| {
                if node.contents.is_duplicate_of(&sibling.contents) {
                    Some(sibling.id)
                } else {
                    None
                }
            })
        })
    }
}

impl<T: IndependentContents, M> crate::IndependentWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    //#[debug_ensures(self.verify())]
    fn replace_node_parents(&mut self, target: u128, parents: &[u128]) -> bool {
        todo!()
    }
}
