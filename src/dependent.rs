use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hash},
};

use contracts::*;
use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64};

use crate::{
    DiscreteContentResult, DiscreteContents, DiscreteWeave, DowContents, DuplicatableContents,
    DuplicatableWeave, Node, Weave,
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

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct DependentWeave<T, M> {
    nodes: HashMap<u128, DependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    active: Option<u128>,
    bookmarked: HashSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T> ArchivedDependentNode<T>
where
    T: Archive,
{
    pub fn partial_deserialize<'a, D>(
        &'a self,
        deserializer: &mut D,
    ) -> Result<DependentNode<DowContents<'a, T>>, D::Error>
    where
        D: rkyv::rancor::Fallible + ?Sized,
        T::Archived: Deserialize<T, D> + Hash + Eq,
    {
        Ok(DependentNode {
            id: self.id.deserialize(deserializer)?,
            from: self.from.deserialize(deserializer)?,
            to: self.to.deserialize(deserializer)?,
            active: self.active.deserialize(deserializer)?,
            bookmarked: self.bookmarked.deserialize(deserializer)?,
            contents: DowContents::Archived(&self.contents),
        })
    }
}

impl<T, M> ArchivedDependentWeave<T, M>
where
    T: Archive,
    M: Archive,
{
    pub fn partial_deserialize<'a, D>(
        &'a self,
        deserializer: &mut D,
    ) -> Result<DependentWeave<DowContents<'a, T>, DowContents<'a, M>>, D::Error>
    where
        D: rkyv::rancor::Fallible + ?Sized,
        T::Archived: Deserialize<T, D> + Hash + Eq,
    {
        let mut nodes =
            HashMap::with_capacity_and_hasher(self.nodes.len(), BuildHasherDefault::default());
        for (k, v) in self.nodes.iter() {
            nodes.insert(
                k.deserialize(deserializer)?,
                v.partial_deserialize(deserializer)?,
            );
        }

        Ok(DependentWeave {
            nodes,
            roots: self.roots.deserialize(deserializer)?,
            active: self.active.deserialize(deserializer)?,
            bookmarked: self.bookmarked.deserialize(deserializer)?,
            metadata: DowContents::Archived(&self.metadata),
        })
    }
}

impl<T, M> DependentWeave<T, M> {
    fn verify(&self) -> bool {
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
    fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
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
    fn siblings(&self, node: &DependentNode<T>) -> impl Iterator<Item = &DependentNode<T>> {
        node.from.iter().copied().flat_map(|id| {
            self.nodes
                .get(&id)
                .into_iter()
                .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
                .filter_map(|id| self.nodes.get(&id))
        })
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

    fn get_active_threads(&self) -> impl Iterator<Item = u128> {
        self.active.into_iter()
    }

    #[debug_ensures(self.verify())]
    #[ensures(self.under_max_size())]
    fn add_node(&mut self, node: DependentNode<T>) -> bool {
        if self.nodes.contains_key(&node.id) || !node.verify() || !node.to.is_empty() {
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

    #[debug_ensures(value == (self.active == Some(*id)))]
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

    #[debug_ensures(value == self.bookmarked.contains(id))]
    #[debug_ensures(self.verify())]
    fn set_node_bookmarked_status(&mut self, id: &u128, value: bool) -> bool {
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.bookmarked = value;
                if value {
                    self.bookmarked.insert(node.id);
                } else {
                    self.bookmarked.remove(id);
                }

                true
            }
            None => false,
        }
    }

    #[debug_ensures(!self.nodes.contains_key(id))]
    #[debug_ensures(self.verify())]
    fn remove_node(&mut self, id: &u128) -> Option<DependentNode<T>> {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.remove(id);
            self.bookmarked.remove(id);
            if node.active {
                self.active = node.from;
                if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                    parent.active = true;
                }
            }
            if let Some(parent) = node.from.and_then(|id| self.nodes.get_mut(&id)) {
                parent.to.remove(id);
            }
            for child in node.to.iter() {
                self.remove_node(child);
            }

            Some(node)
        } else {
            None
        }
    }
}

impl<T: DiscreteContents, M> DiscreteWeave<DependentNode<T>, T> for DependentWeave<T, M> {
    #[debug_ensures(self.verify())]
    #[ensures(self.under_max_size())]
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
                        to: HashSet::from_iter([new_id]),
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

                        self.nodes.insert(parent.id, parent);
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

impl<T: DuplicatableContents, M> DuplicatableWeave<DependentNode<T>, T> for DependentWeave<T, M> {
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
