//! WIP

use std::{
    collections::{HashMap, HashSet, VecDeque},
    hash::BuildHasherDefault,
};

use contracts::*;
use indexmap::IndexSet;
use rkyv::{Archive, Deserialize, Serialize, hash::FxHasher64, rend::u128_le};

use crate::{
    ArchivedNode, ArchivedWeave, DeduplicatableContents, DiscreteContents, DiscreteWeave,
    DuplicatableWeave, IndependentContents, Node, Weave,
};

#[derive(Archive, Deserialize, Serialize, Debug)]
pub struct IndependentNode<T>
where
    T: IndependentContents,
{
    pub id: u128,
    pub from: IndexSet<u128, BuildHasherDefault<FxHasher64>>,
    pub to: IndexSet<u128, BuildHasherDefault<FxHasher64>>,

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
    roots: IndexSet<u128, BuildHasherDefault<FxHasher64>>,
    active: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    bookmarked: IndexSet<u128, BuildHasherDefault<FxHasher64>>,

    pub metadata: M,
}

impl<T, M> IndependentWeave<T, M>
where
    T: IndependentContents,
{
    fn verify(&self) -> bool {
        let nodes: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();
        let nodes_std: HashSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();
        let active_index: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
            self.active.iter().copied().collect();
        let roots: Vec<u128> = self.roots.iter().copied().collect();

        //self.roots.is_subset(&nodes)
        self.verify_layer(&roots)
            && self.active.is_subset(&nodes_std)
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
                        !value.from.is_disjoint(&active_index)
                    } else {
                        true
                    }
            })
    }
    fn verify_layer(&self, layer: &[u128]) -> bool {
        let mut next_layer = Vec::new();
        let mut has_active = false;

        for node in layer {
            if let Some(node) = self.nodes.get(node) {
                next_layer.extend(node.to.iter().copied());

                if node.active {
                    if !has_active {
                        has_active = true;
                    } else {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        if !next_layer.is_empty() {
            self.verify_layer(&next_layer)
        } else {
            true
        }
    }
    fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
}

impl<T: IndependentContents, M> IndependentWeave<T, M> {
    pub fn with_capacity(capacity: usize, metadata: M) -> Self {
        Self {
            nodes: HashMap::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            roots: IndexSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            active: HashSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            bookmarked: IndexSet::with_capacity_and_hasher(capacity, BuildHasherDefault::default()),
            metadata,
        }
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
    fn active_parents(
        &self,
        node: &IndependentNode<T>,
    ) -> impl Iterator<Item = &IndependentNode<T>> {
        node.from
            .iter()
            .filter_map(|id| self.nodes.get(id))
            .filter(|parent| parent.active)
    }
    fn all_parents(&self, node: &IndependentNode<T>) -> impl Iterator<Item = &IndependentNode<T>> {
        node.from.iter().filter_map(|id| self.nodes.get(id))
    }
    fn all_parents_or_roots<'a>(
        &'a self,
        node: &'a IndependentNode<T>,
    ) -> Box<dyn Iterator<Item = &'a IndependentNode<T>> + 'a> {
        if node.from.is_empty() {
            Box::new(self.roots.iter().filter_map(|id| self.nodes.get(id)))
        } else {
            Box::new(node.from.iter().filter_map(|id| self.nodes.get(id)))
        }
    }
    fn siblings_from_active_parents(
        &self,
        node: &IndependentNode<T>,
    ) -> impl Iterator<Item = &IndependentNode<T>> {
        self.active_parents(node)
            .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
            .filter_map(|id| self.nodes.get(&id))
    }
    fn siblings_from_all_parents_including_roots<'a>(
        &'a self,
        node: &'a IndependentNode<T>,
    ) -> Box<dyn Iterator<Item = &'a IndependentNode<T>> + 'a> {
        if node.from.is_empty() {
            Box::new(self.roots.iter().filter_map(|id| self.nodes.get(id)))
        } else {
            Box::new(
                self.all_parents(node)
                    .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
                    .filter_map(|id| self.nodes.get(&id)),
            )
        }
    }
    //#[debug_ensures(self.verify())]
    fn update_node_activity_in_place(&mut self, id: &u128, value: bool) -> bool {
        if let Some(node) = self.nodes.get(id) {
            if node.active == value {
                return true;
            }

            if value {
                let has_active_parents =
                    self.all_parents_or_roots(node).any(|parent| parent.active);
                if has_active_parents {
                    let siblings: Vec<u128> = self
                        .siblings_from_all_parents_including_roots(node)
                        .filter(|sibling| sibling.active)
                        .map(|sibling| sibling.id)
                        .collect();

                    for sibling in siblings {
                        self.update_node_activity_in_place(&sibling, false);
                    }
                } else if let Some(child) = node.from.first().copied() {
                    self.update_node_activity_in_place(&child, true);
                }
            } else {
                let selected_children: Vec<u128> = node
                    .to
                    .iter()
                    .copied()
                    .filter(|id| {
                        !self
                            .nodes
                            .get(id)
                            .iter()
                            .flat_map(|child| child.from.iter().filter_map(|id| self.nodes.get(id)))
                            .any(|child_parent| child_parent.active && child_parent.id != node.id)
                    })
                    .collect();

                for child in selected_children {
                    self.update_node_activity_in_place(&child, false);
                }
            }
        }
        match self.nodes.get_mut(id) {
            Some(node) => {
                node.active = value;
                if value {
                    self.active.insert(node.id);
                } else {
                    self.active.remove(&node.id);
                }
                true
            }
            None => false,
        }
    }
    fn deactivate_top_level_node_recursive(&mut self, id: &u128) -> bool {
        if let Some(node) = self.nodes.get_mut(id) {
            if !node.active {
                return true;
            }
            node.active = false;
            self.active.remove(&node.id);

            let parents: Vec<u128> = node.from.iter().copied().collect();

            for parent in parents {
                self.deactivate_top_level_node_recursive(&parent);
            }

            true
        } else {
            false
        }
    }
    fn update_removed_child_activity(&mut self, id: &u128) -> bool {
        if let Some(node) = self.nodes.get(id) {
            if !node.active {
                return true;
            }

            let has_active_parents = node
                .from
                .iter()
                .filter_map(|id| self.nodes.get(id))
                .any(|parent| parent.active);

            if has_active_parents {
                return true;
            }
        }
        if let Some(node) = self.nodes.get_mut(id) {
            node.active = false;
            self.active.remove(&node.id);

            let children: Vec<u128> = node.to.iter().copied().collect();
            for child in &children {
                self.update_removed_child_activity(child);
            }

            true
        } else {
            false
        }
    }
    fn build_thread(&self, id: &u128, thread: &mut VecDeque<u128>) {
        if let Some(node) = self.nodes.get(id)
            && node.active
        {
            thread.push_back(*id);

            for child in &node.from {
                self.build_thread_children(child, thread);
            }

            for parent in &node.to {
                self.build_thread_parents(parent, thread);
            }
        }
    }
    fn build_thread_children(&self, id: &u128, thread: &mut VecDeque<u128>) {
        if let Some(node) = self.nodes.get(id)
            && node.active
        {
            thread.push_back(*id);

            for child in &node.from {
                self.build_thread_children(child, thread);
            }
        }
    }
    fn build_thread_parents(&self, id: &u128, thread: &mut VecDeque<u128>) {
        if let Some(node) = self.nodes.get(id)
            && node.active
        {
            thread.push_front(*id);

            for parent in &node.to {
                self.build_thread_parents(parent, thread);
            }
        }
    }
    #[debug_ensures(!self.nodes.contains_key(id))]
    fn remove_node_unverified(&mut self, id: &u128) -> Option<IndependentNode<T>> {
        if let Some(node) = self.nodes.remove(id) {
            self.roots.shift_remove(id);
            self.bookmarked.shift_remove(id);
            self.active.remove(id);
            for parent in &node.from {
                if let Some(parent) = self.nodes.get_mut(parent) {
                    parent.to.shift_remove(&node.id);
                }
            }
            for child in &node.to {
                if let Some(child) = self.nodes.get_mut(child) {
                    child.from.shift_remove(&node.id);

                    let identifier = child.id;
                    if child.from.is_empty() {
                        self.remove_node_unverified(&identifier);
                    } else if node.active && child.active {
                        self.update_removed_child_activity(&identifier);
                    }
                }
            }
            Some(node)
        } else {
            None
        }
    }
}

impl<T: IndependentContents, M> Weave<IndependentNode<T>, T> for IndependentWeave<T, M> {
    fn len(&self) -> usize {
        self.nodes.len()
    }
    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }
    fn contains(&self, id: &u128) -> bool {
        self.nodes.contains_key(id)
    }
    fn get_node(&self, id: &u128) -> Option<&IndependentNode<T>> {
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
            VecDeque::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        if let Some(active) = self.active.iter().last() {
            self.build_thread(active, &mut thread);
        }

        thread.into_iter()
    }
    #[debug_ensures(self.verify())]
    #[requires(self.under_max_size())]
    fn add_node(&mut self, mut node: IndependentNode<T>) -> bool {
        let is_invalid = self.nodes.contains_key(&node.id)
            || !node.verify()
            || !node.from.iter().all(|id| self.nodes.contains_key(id))
            || !node.to.iter().all(|id| self.nodes.contains_key(id));

        if is_invalid {
            return false;
        }

        for child in &node.to {
            let child = self.nodes.get(child).unwrap();
            if child.from.is_empty() && child.active {
                node.active = true;
                self.roots.shift_remove(&child.id);
            }
        }

        if node.from.is_empty() {
            if node.active {
                let roots: Vec<u128> = self.roots.iter().copied().collect();

                for root in &roots {
                    let is_active = self.nodes.get(root).unwrap().active;

                    if is_active {
                        self.update_node_activity_in_place(root, false);
                    }
                }
            }

            self.roots.insert(node.id);
        } else {
            if node.active {
                let has_active_parents = node
                    .from
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .any(|parent| parent.active);

                if !has_active_parents {
                    let parent = node.from.first().unwrap();
                    self.update_node_activity_in_place(parent, true);
                }

                let siblings: Vec<u128> = node
                    .from
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .flat_map(|parent| parent.to.iter().copied().filter(|id| *id != node.id))
                    .filter_map(|id| self.nodes.get(&id))
                    .filter(|sibling| sibling.active)
                    .map(|sibling| sibling.id)
                    .collect();

                for sibling in siblings {
                    self.update_node_activity_in_place(&sibling, false);
                }
            }

            for parent in &node.from {
                let parent = self.nodes.get_mut(parent).unwrap();
                parent.to.insert(node.id);
            }
        }

        for child in &node.to {
            let child = self.nodes.get_mut(child).unwrap();
            child.from.insert(node.id);
        }

        if node.active {
            self.active.insert(node.id);
        }

        if node.bookmarked {
            self.bookmarked.insert(node.id);
        }

        self.nodes.insert(node.id, node);

        true
    }
    #[debug_ensures((ret && value == self.active.contains(id)) || !ret)]
    #[debug_ensures(self.verify())]
    fn set_node_active_status(&mut self, id: &u128, value: bool) -> bool {
        let top_level_deactivation = if !value && let Some(node) = self.nodes.get(id) {
            if node.active {
                let has_active_children = node
                    .to
                    .iter()
                    .filter_map(|id| self.nodes.get(id))
                    .any(|child| child.active);

                !has_active_children
            } else {
                false
            }
        } else {
            false
        };

        if top_level_deactivation {
            self.deactivate_top_level_node_recursive(id)
        } else {
            self.update_node_activity_in_place(id, value)
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
    fn remove_node(&mut self, id: &u128) -> Option<IndependentNode<T>> {
        self.remove_node_unverified(id)
    }
}

impl<T: DiscreteContents + IndependentContents, M> DiscreteWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    //#[debug_ensures(self.verify())]
    //#[requires(self.under_max_size())]
    fn split_node(&mut self, id: &u128, at: usize, new_id: u128) -> bool {
        todo!()
    }
    //#[debug_ensures(self.verify())]
    fn merge_with_parent(&mut self, id: &u128) -> bool {
        todo!()
    }
}

impl<T: DeduplicatableContents + IndependentContents, M> DuplicatableWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    fn find_duplicates(&self, id: &u128) -> impl Iterator<Item = u128> {
        self.nodes.get(id).into_iter().flat_map(|node| {
            let iter: Box<dyn Iterator<Item = &IndependentNode<T>>> =
                if node.active && !node.from.is_empty() {
                    Box::new(self.siblings_from_active_parents(node))
                } else {
                    Box::new(self.siblings_from_all_parents_including_roots(node))
                };

            iter.filter_map(|sibling| {
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
    fn move_node(&mut self, target: &u128, parents: &[u128]) -> bool {
        todo!()
    }
}

impl<T> ArchivedNode<T> for ArchivedIndependentNode<T>
where
    T: Archive<Archived = T> + IndependentContents,
{
    fn id(&self) -> u128_le {
        self.id
    }
    fn from(&self) -> impl Iterator<Item = u128_le> {
        self.from.iter().copied()
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

impl<T, M> ArchivedWeave<ArchivedIndependentNode<T>, T> for ArchivedIndependentWeave<T, M>
where
    T: Archive<Archived = T> + IndependentContents,
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
    fn get_node(&self, id: &u128_le) -> Option<&ArchivedIndependentNode<T>> {
        self.nodes.get(id)
    }
    fn get_roots(&self) -> impl Iterator<Item = u128_le> {
        self.roots.iter().copied()
    }
    fn get_bookmarks(&self) -> impl Iterator<Item = u128_le> {
        self.bookmarked.iter().copied()
    }
    fn get_active_threads(&self) -> impl Iterator<Item = u128_le> {
        self.active.iter().copied()
    }
}
