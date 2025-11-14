use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasherDefault,
};

use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

use crate::{
    DiscreteContents, DiscreteWeave, DuplicatableContents, DuplicatableWeave, Node, Weave,
};

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct DependentNode<T> {
    pub id: u128,
    pub from: Option<u128>,
    pub to: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub active: bool,
    pub bookmarked: bool,
    pub contents: T,
}

impl<T> DependentNode<T> {
    /// TODO: Replace this with a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features
    pub fn verify(&self) -> bool {
        (if let Some(from) = self.from {
            !self.to.contains(&from)
        } else {
            true
        } && self.from != Some(self.id)
            && !self.to.contains(&self.id))
    }
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

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct DependentWeave<T, M> {
    nodes: HashMap<u128, DependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    active: Option<u128>,
    bookmarked: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T, M> DependentWeave<T, M> {
    /// TODO: Replace this with a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features
    pub fn verify(&self) -> bool {
        let nodes: HashSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();

        self.roots.is_subset(&nodes)
            && if let Some(active) = self.active {
                self.nodes.contains_key(&active)
            } else {
                true
            }
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.verify()
                    && value.id == *key
                    && if let Some(from) = value.from {
                        self.nodes.contains_key(&from)
                    } else {
                        true
                    }
                    && value.to.is_subset(&nodes)
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
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
                        .all(|p| p.from == Some(*key))
            })
    }
}

impl<T, M> DependentWeave<T, M> {
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: None,
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
        self.bookmarked.reserve(additional);
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
    }
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
        if value
            && self.nodes.contains_key(&id)
            && let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id))
        {
            active.active = false;
        }

        match self.nodes.get_mut(&id) {
            Some(node) => {
                if value {
                    self.active = Some(id)
                } else if self.active == Some(id) {
                    self.active = node.from;
                }

                node.active = value;

                debug_assert!(self.verify());
                true
            }
            None => {
                debug_assert!(self.verify());
                false
            }
        }
    }

    fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool {
        match self.nodes.get_mut(&id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(id);
                } else {
                    self.bookmarked.remove(&id);
                }

                debug_assert!(self.verify());
                true
            }
            None => false,
        }
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
