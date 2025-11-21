use std::{collections::HashMap, hash::BuildHasherDefault};

use contracts::*;
use indexmap::IndexSet;
use rkyv::{
    Archive, Deserialize, Serialize, hash::FxHasher64, option::ArchivedOption, rend::u128_le,
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::{
    ArchivedNode, ArchivedWeave, DeduplicatableContents, DiscreteContentResult, DiscreteContents,
    DiscreteWeave, DuplicatableWeave, IndependentContents, Node, SemiIndependentWeave, Weave,
};

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct DependentNode<T> {
    pub id: u128,
    pub from: Option<u128>,
    pub to: IndexSet<u128, BuildHasherDefault<FxHasher64>>,

    pub active: bool,
    pub bookmarked: bool,
    pub contents: T,
}

impl<T> DependentNode<T> {
    fn verify(&self) -> bool {
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

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct DependentWeave<T, M> {
    nodes: HashMap<u128, DependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: IndexSet<u128, BuildHasherDefault<FxHasher64>>,
    active: Option<u128>,
    bookmarked: IndexSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T, M> DependentWeave<T, M> {
    fn verify(&self) -> bool {
        let nodes: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
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
    fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
}

impl<T, M> DependentWeave<T, M> {
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: None,
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            metadata,
        }
    }
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
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
    fn siblings<'a>(
        &'a self,
        node: &'a DependentNode<T>,
    ) -> Box<dyn Iterator<Item = &'a DependentNode<T>> + 'a> {
        match &node.from {
            Some(parent) => Box::new(self.nodes.get(parent).into_iter().flat_map(|parent| {
                parent
                    .to
                    .iter()
                    .copied()
                    .filter(|id| *id != node.id)
                    .filter_map(|id| self.nodes.get(&id))
            })),
            None => Box::new(
                self.roots
                    .iter()
                    .copied()
                    .filter(|id| *id != node.id)
                    .filter_map(|id| self.nodes.get(&id)),
            ),
        }
    }
    #[debug_ensures(!self.nodes.contains_key(id))]
    fn remove_node_unverified(&mut self, id: &u128) -> Option<DependentNode<T>> {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            if node.active {
                self.active = node.from;
                if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                    parent.active = true;
                }
            }
            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                parent.to.shift_remove(id);
            }
            for child in node.to.iter() {
                self.remove_node_unverified(child);
            }

            Some(node)
        } else {
            None
        }
    }
    fn build_thread(&self, id: &u128, thread: &mut Vec<u128>) {
        if let Some(node) = self.nodes.get(id) {
            thread.push(*id);
            if let Some(parent) = node.from {
                self.build_thread(&parent, thread);
            }
        }
    }
}

impl<T, M> Weave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn contains(&self, id: &u128) -> bool {
        self.nodes.contains_key(id)
    }
    fn get_node(&self, id: &u128) -> Option<&DependentNode<T>> {
        self.nodes.get(id)
    }
    fn get_roots(&self) -> impl Iterator<Item = u128> {
        self.roots.iter().copied()
    }
    fn get_bookmarks(&self) -> impl Iterator<Item = u128> {
        self.bookmarked.iter().copied()
    }
    fn get_active_thread(&self) -> impl Iterator<Item = u128> {
        let mut thread =
            Vec::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        if let Some(active) = self.active {
            self.build_thread(&active, &mut thread);
        }

        thread.into_iter()
    }
    #[debug_ensures(self.verify())]
    #[requires(self.under_max_size())]
    fn add_node(&mut self, node: DependentNode<T>) -> bool {
        let is_invalid = self.nodes.contains_key(&node.id) || !node.verify() || !node.to.is_empty();

        if is_invalid {
            return false;
        }

        if let Some(from) = node.from {
            match self.nodes.get_mut(&from) {
                Some(parent) => {
                    parent.to.insert(node.id);
                }
                None => return false,
            }
        }

        if node.active {
            if let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                active.active = false;
            }

            self.active = Some(node.id);
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        self.nodes.insert(node.id, node);

        true
    }
    #[debug_ensures((ret && value == (self.active == Some(*id))) || !ret)]
    #[debug_ensures(self.verify())]
    fn set_node_active_status(&mut self, id: &u128, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.active = value;

                if value {
                    if let Some(active) = self.active.and_then(|id| self.nodes.get_mut(&id)) {
                        active.active = false;
                    }

                    self.active = Some(*id);
                } else if self.active == Some(node.id) {
                    self.active = None;
                }

                true
            }
            None => false,
        }
    }
    #[debug_ensures((ret && value == self.bookmarked.contains(id)) || !ret)]
    #[debug_ensures(self.verify())]
    fn set_node_bookmarked_status(&mut self, id: &u128, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(node.id);
                } else {
                    self.bookmarked.shift_remove(id);
                }

                true
            }
            None => false,
        }
    }
    #[debug_ensures(!self.nodes.contains_key(id))]
    #[debug_ensures(self.verify())]
    fn remove_node(&mut self, id: &u128) -> Option<DependentNode<T>> {
        self.remove_node_unverified(id)
    }
}

impl<T: DiscreteContents, M> DiscreteWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    #[debug_ensures(self.verify())]
    #[requires(self.under_max_size())]
    fn split_node(&mut self, id: &u128, at: usize, new_id: u128) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two((left, right)) => {
                    let left_node = DependentNode {
                        id: node.id,
                        from: node.from,
                        to: IndexSet::from_iter([new_id]),
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = Some(node.id);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    for child in node.to.iter() {
                        let child = self.nodes.get_mut(child).unwrap();
                        child.from = Some(node.id);
                    }

                    self.nodes.insert(left_node.id, left_node);
                    self.nodes.insert(node.id, node);

                    true
                }
                DiscreteContentResult::One(content) => {
                    node.contents = content;
                    self.nodes.insert(node.id, node);
                    false
                }
            }
        } else {
            false
        }
    }
    #[debug_ensures(self.verify())]
    fn merge_with_parent(&mut self, id: &u128) -> bool {
        if let Some(mut node) = self.nodes.remove(id) {
            if let Some(mut parent) = node.from.and_then(|id| self.nodes.remove(&id)) {
                if parent.to.len() > 1 {
                    self.nodes.insert(parent.id, parent);
                    self.nodes.insert(node.id, node);
                    return false;
                }

                match parent.contents.merge(node.contents) {
                    DiscreteContentResult::Two((left, right)) => {
                        parent.contents = left;
                        node.contents = right;
                        self.nodes.insert(parent.id, parent);
                        self.nodes.insert(node.id, node);
                        false
                    }
                    DiscreteContentResult::One(content) => {
                        parent.contents = content;
                        parent.to = node.to;

                        for child in parent.to.iter() {
                            let child = self.nodes.get_mut(child).unwrap();
                            child.from = Some(parent.id);
                        }

                        if node.active {
                            parent.active = true;
                            self.active = Some(parent.id);
                        }

                        self.nodes.insert(parent.id, parent);

                        self.bookmarked.shift_remove(&node.id);

                        true
                    }
                }
            } else {
                self.nodes.insert(node.id, node);
                false
            }
        } else {
            false
        }
    }
}

impl<T: IndependentContents, M> SemiIndependentWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn get_contents_mut(&mut self, id: &u128) -> Option<&mut T> {
        self.nodes.get_mut(id).map(|node| &mut node.contents)
    }
}

impl<T: DeduplicatableContents, M> DuplicatableWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    fn find_duplicates(&self, id: &u128) -> impl Iterator<Item = u128> {
        self.nodes.get(id).into_iter().flat_map(|node| {
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

impl<T> ArchivedNode<T> for ArchivedDependentNode<T>
where
    T: Archive<Archived = T>,
{
    fn id(&self) -> u128_le {
        self.id
    }
    fn from(&self) -> impl Iterator<Item = u128_le> {
        self.from.into_iter().copied()
    }
    fn to(&self) -> impl Iterator<Item = u128_le> {
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

impl<T, M> ArchivedDependentWeave<T, M>
where
    T: Archive<Archived = T>,
    M: Archive<Archived = T>,
{
    fn build_thread(&self, id: &u128_le, thread: &mut Vec<u128_le>) {
        if let Some(node) = self.nodes.get(id) {
            thread.push(*id);
            if let ArchivedOption::Some(parent) = node.from {
                self.build_thread(&parent, thread);
            }
        }
    }
}

impl<T, M> ArchivedWeave<ArchivedDependentNode<T>, T> for ArchivedDependentWeave<T, M>
where
    T: Archive<Archived = T>,
    M: Archive<Archived = T>,
{
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn contains(&self, id: &u128_le) -> bool {
        self.nodes.contains_key(id)
    }
    fn get_node(&self, id: &u128_le) -> Option<&ArchivedDependentNode<T>> {
        self.nodes.get(id)
    }
    fn get_roots(&self) -> impl Iterator<Item = u128_le> {
        self.roots.iter().copied()
    }
    fn get_bookmarks(&self) -> impl Iterator<Item = u128_le> {
        self.bookmarked.iter().copied()
    }
    fn get_active_thread(&self) -> impl Iterator<Item = u128_le> {
        let mut thread =
            Vec::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        if let ArchivedOption::Some(active) = self.active {
            self.build_thread(&active, &mut thread);
        }

        thread.into_iter()
    }
}
