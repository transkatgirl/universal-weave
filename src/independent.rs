//! Experimental & untested; likely contains serious bugs

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    hash::BuildHasherDefault,
};

use contracts::*;
use indexmap::IndexSet;
use rkyv::{
    Archive, Deserialize, Serialize,
    collections::swiss_table::{ArchivedHashMap, ArchivedHashSet, ArchivedIndexSet},
    hash::FxHasher64,
    rend::u128_le,
};

#[cfg(feature = "serde")]
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};

use crate::{
    ArchivedNode, ArchivedWeave, DeduplicatableContents, DiscreteContentResult, DiscreteContents,
    DiscreteWeave, DuplicatableWeave, IndependentContents, Node, Weave,
};

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
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
    fn validate(&self) -> bool {
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

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[cfg_attr(feature = "serde", derive(SerdeSerialize, SerdeDeserialize))]
pub struct IndependentWeave<T, M>
where
    T: IndependentContents,
{
    nodes: HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    roots: IndexSet<u128, BuildHasherDefault<FxHasher64>>,
    active: HashSet<u128, BuildHasherDefault<FxHasher64>>,
    bookmarked: IndexSet<u128, BuildHasherDefault<FxHasher64>>,
    thread: VecDeque<u128>,

    pub metadata: M,
}

impl<T, M> IndependentWeave<T, M>
where
    T: IndependentContents,
{
    pub fn validate(&self) -> bool {
        let nodes: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();
        let nodes_std: HashSet<u128, BuildHasherDefault<FxHasher64>> =
            self.nodes.keys().copied().collect();
        let active_index: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
            self.active.iter().copied().collect();
        let roots: Vec<u128> = self.roots.iter().copied().collect();

        //self.roots.is_subset(&nodes)
        self.validate_layer(&roots)
            && self.active.is_subset(&nodes_std)
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
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
    fn validate_layer(&self, layer: &[u128]) -> bool {
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
            self.validate_layer(&next_layer)
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
            thread: VecDeque::with_capacity(capacity),
            metadata,
        }
    }
    pub fn capacity(&self) -> usize {
        self.nodes.capacity()
    }
    pub fn reserve(&mut self, additional: usize) {
        self.nodes.reserve(additional);
        self.roots
            .reserve(self.nodes.capacity().saturating_sub(self.roots.capacity()));
        self.active
            .reserve(self.nodes.capacity().saturating_sub(self.active.capacity()));
        self.bookmarked.reserve(
            self.nodes
                .capacity()
                .saturating_sub(self.bookmarked.capacity()),
        );
        self.thread
            .reserve(self.nodes.capacity().saturating_sub(self.thread.capacity()));
    }
    pub fn shrink_to(&mut self, min_capacity: usize) {
        self.nodes.shrink_to(min_capacity);
        self.roots.shrink_to(min_capacity);
        self.active.shrink_to(min_capacity);
        self.bookmarked.shrink_to(min_capacity);
        self.thread.shrink_to(min_capacity);
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
    //#[debug_ensures(self.validate())]
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
    /*fn deactivate_top_level_node_recursive(&mut self, id: &u128) -> bool {
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
    }*/
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
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = u128> {
        self.nodes.keys().copied()
    }
    fn get_roots(&self) -> &IndexSet<u128, BuildHasherDefault<FxHasher64>> {
        &self.roots
    }
    fn get_bookmarks(&self) -> &IndexSet<u128, BuildHasherDefault<FxHasher64>> {
        &self.bookmarked
    }
    fn get_active_thread(&mut self) -> &VecDeque<u128> {
        self.thread.clear();

        if let Some(active) = self.active.iter().last() {
            build_thread(&self.nodes, active, &mut self.thread);
        }

        &self.thread
    }
    fn get_thread_from(&mut self, id: &u128) -> &VecDeque<u128> {
        self.thread.clear();

        build_thread_from(&self.nodes, &self.active, id, &mut self.thread);

        &self.thread
    }
    #[debug_ensures(self.validate())]
    #[requires(self.under_max_size())]
    fn add_node(&mut self, mut node: IndependentNode<T>) -> bool {
        let is_invalid = self.nodes.contains_key(&node.id)
            || !node.validate()
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
    #[debug_ensures(self.validate())]
    fn set_node_active_status(&mut self, id: &u128, value: bool, alternate: bool) -> bool {
        if value
            && let Some(node) = self.nodes.get(id)
            && let Some(active_child) = node
                .to
                .iter()
                .filter_map(|child| self.nodes.get(child))
                .find(|child| child.active)
        {
            let child_id = active_child.id;

            if (!alternate && active_child.from.len() == 1)
                || (alternate && active_child.from.len() > 1)
            {
                let result = self.update_node_activity_in_place(id, true);
                self.update_node_activity_in_place(&child_id, false);

                result
            } else {
                self.update_node_activity_in_place(id, value)
            }
        } else {
            self.update_node_activity_in_place(id, value)
        }

        /*let top_level_deactivation = if !value && let Some(node) = self.nodes.get(id) {
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
        }*/
    }
    #[debug_ensures((ret && value == self.bookmarked.contains(id)) || !ret)]
    #[debug_ensures(self.validate())]
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
    #[debug_ensures(ret == self.contains(id))]
    #[debug_ensures(self.validate())]
    fn sort_node_children_by(
        &mut self,
        id: &u128,
        mut compare: impl FnMut(&IndependentNode<T>, &IndependentNode<T>) -> Ordering,
    ) -> bool {
        if let Some(node) = self.nodes.get(id) {
            let mut children: Vec<_> = node.to.iter().filter_map(|id| self.nodes.get(id)).collect();
            children.sort_by(|a, b| compare(a, b));

            let children: IndexSet<u128, BuildHasherDefault<FxHasher64>> =
                children.into_iter().map(|node| node.id).collect();

            if let Some(node) = self.nodes.get_mut(id) {
                node.to = children;

                true
            } else {
                false
            }
        } else {
            false
        }
    }
    #[debug_ensures(self.validate())]
    fn sort_roots_by(
        &mut self,
        mut compare: impl FnMut(&IndependentNode<T>, &IndependentNode<T>) -> Ordering,
    ) {
        let mut roots: Vec<_> = self
            .roots
            .iter()
            .filter_map(|id| self.nodes.get(id))
            .collect();
        roots.sort_by(|a, b| compare(a, b));

        self.roots = roots.into_iter().map(|node| node.id).collect();
    }
    #[debug_ensures(!self.nodes.contains_key(id))]
    #[debug_ensures(self.validate())]
    fn remove_node(&mut self, id: &u128) -> Option<IndependentNode<T>> {
        self.remove_node_unverified(id)
    }
}

impl<T: DiscreteContents + IndependentContents, M> DiscreteWeave<IndependentNode<T>, T>
    for IndependentWeave<T, M>
{
    #[debug_ensures(self.validate())]
    #[requires(self.under_max_size())]
    fn split_node(&mut self, id: &u128, at: usize, new_id: u128) -> bool {
        if self.nodes.contains_key(&new_id) || *id == new_id {
            return false;
        }

        if let Some(mut node) = self.nodes.remove(id) {
            match node.contents.split(at) {
                DiscreteContentResult::Two((left, right)) => {
                    let left_node = IndependentNode {
                        id: node.id,
                        from: node.from,
                        to: IndexSet::from_iter([new_id]),
                        active: node.active,
                        bookmarked: node.bookmarked,
                        contents: left,
                    };

                    node.from = IndexSet::from_iter([node.id]);
                    node.id = new_id;
                    node.contents = right;
                    node.active = false;
                    node.bookmarked = false;

                    for child in node.to.iter() {
                        let child = self.nodes.get_mut(child).unwrap();

                        if let Some(index) = child.from.get_index_of(&left_node.id) {
                            if child.from.replace_index(index, node.id).is_err() {
                                child.from.shift_remove_index(index);
                            }
                        } else {
                            child.from.insert(node.id);
                        }
                        if child.active {
                            node.active = true;
                        }
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
    #[debug_ensures(self.validate())]
    fn merge_with_parent(&mut self, id: &u128) -> bool {
        if let Some(mut node) = self.nodes.remove(id) {
            if node.from.len() != 1 {
                self.nodes.insert(node.id, node);
                return false;
            }

            if let Some(mut parent) = node.from.first().and_then(|id| self.nodes.remove(id)) {
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

                            if let Some(index) = child.from.get_index_of(&node.id) {
                                if child.from.replace_index(index, parent.id).is_err() {
                                    child.from.shift_remove_index(index);
                                }
                            } else {
                                child.from.insert(parent.id);
                            }
                        }

                        self.nodes.insert(parent.id, parent);

                        self.bookmarked.shift_remove(&node.id);
                        self.active.remove(&node.id);

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
    #[debug_ensures(self.validate())]
    fn move_node(&mut self, id: &u128, new_parents: &[u128]) -> bool {
        let mut has_active_new_parents = false;

        for new_parent in new_parents {
            match self.nodes.get(new_parent) {
                Some(new_parent) => {
                    if new_parent.active {
                        has_active_new_parents = true;
                    }
                }
                None => {
                    return false;
                }
            }
        }

        let new_parents = IndexSet::from_iter(new_parents.iter().copied());

        if new_parents.contains(id) {
            return false;
        }

        if let Some(node) = self.nodes.get(id) {
            for child in &node.to {
                if new_parents.contains(child) {
                    return false;
                }
            }

            let old_parents = node.from.clone();

            for old_parent in &old_parents {
                if !new_parents.contains(old_parent)
                    && let Some(old_parent) = self.nodes.get_mut(old_parent)
                {
                    old_parent.to.shift_remove(id);
                }
            }

            for new_parent in &new_parents {
                if !old_parents.contains(new_parent)
                    && let Some(new_parent) = self.nodes.get_mut(new_parent)
                {
                    new_parent.to.insert(*id);
                }
            }
        } else {
            return false;
        }

        let node = self.nodes.get_mut(id).unwrap();
        node.from = new_parents;

        if node.from.is_empty() {
            self.roots.insert(node.id);
        } else {
            self.roots.shift_remove(&node.id);
        }

        if node.active
            && !has_active_new_parents
            && let Some(first_parent) = node.from.first().copied()
        {
            assert!(self.update_node_activity_in_place(&first_parent, true));
        }

        true
    }
    fn get_contents_mut(&mut self, id: &u128) -> Option<&mut T> {
        self.nodes.get_mut(id).map(|node| &mut node.contents)
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
    fn get_all_nodes_unordered(&self) -> impl ExactSizeIterator<Item = u128_le> {
        self.nodes.keys().copied()
    }
    fn get_roots(&self) -> &ArchivedIndexSet<u128_le> {
        &self.roots
    }
    fn get_bookmarks(&self) -> &ArchivedIndexSet<u128_le> {
        &self.bookmarked
    }
    fn get_active_thread(&self) -> VecDeque<u128_le> {
        let mut thread =
            VecDeque::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        if let Some(active) = self.active.iter().last() {
            build_thread_archived(&self.nodes, active, &mut thread);
        }

        thread
    }
    fn get_thread_from(&self, id: &u128_le) -> VecDeque<u128_le> {
        let mut thread =
            VecDeque::with_capacity((self.nodes.len() as f32).sqrt().max(16.0).round() as usize);

        build_thread_from_archived(&self.nodes, &self.active, id, &mut thread);

        thread
    }
}

fn build_thread<T>(
    nodes: &HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    id: &u128,
    thread: &mut VecDeque<u128>,
) where
    T: IndependentContents,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_back(*id);

        for child in &node.from {
            build_thread_children(nodes, child, thread);
        }

        for parent in &node.to {
            build_thread_parents(nodes, parent, thread);
        }
    }
}

fn build_thread_from<T>(
    nodes: &HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    active: &HashSet<u128, BuildHasherDefault<FxHasher64>>,
    id: &u128,
    thread: &mut VecDeque<u128>,
) where
    T: IndependentContents,
{
    if let Some(node) = nodes.get(id) {
        thread.push_back(*id);

        let mut has_child = false;

        for child in &node.from {
            if active.contains(child) {
                has_child = true;
                build_thread_from(nodes, active, child, thread);
            }
        }

        if !has_child && let Some(child) = node.from.first() {
            build_thread_from(nodes, active, child, thread);
        }
    }
}

fn build_thread_children<T>(
    nodes: &HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    id: &u128,
    thread: &mut VecDeque<u128>,
) where
    T: IndependentContents,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_back(*id);

        for child in &node.from {
            build_thread_children(nodes, child, thread);
        }
    }
}

fn build_thread_parents<T>(
    nodes: &HashMap<u128, IndependentNode<T>, BuildHasherDefault<FxHasher64>>,
    id: &u128,
    thread: &mut VecDeque<u128>,
) where
    T: IndependentContents,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_front(*id);

        for parent in &node.to {
            build_thread_parents(nodes, parent, thread);
        }
    }
}

fn build_thread_archived<T>(
    nodes: &ArchivedHashMap<u128_le, ArchivedIndependentNode<T>>,
    id: &u128_le,
    thread: &mut VecDeque<u128_le>,
) where
    T: IndependentContents + Archive,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_back(*id);

        for child in node.from.iter() {
            build_thread_children_archived(nodes, child, thread);
        }

        for parent in node.to.iter() {
            build_thread_parents_archived(nodes, parent, thread);
        }
    }
}

fn build_thread_from_archived<T>(
    nodes: &ArchivedHashMap<u128_le, ArchivedIndependentNode<T>>,
    active: &ArchivedHashSet<u128_le>,
    id: &u128_le,
    thread: &mut VecDeque<u128_le>,
) where
    T: IndependentContents + Archive,
{
    if let Some(node) = nodes.get(id) {
        thread.push_back(*id);

        let mut has_child = false;

        for child in node.from.iter() {
            if active.contains(child) {
                has_child = true;
                build_thread_from_archived(nodes, active, child, thread);
            }
        }

        if !has_child && let Some(child) = node.from.get_index(0) {
            build_thread_from_archived(nodes, active, child, thread);
        }
    }
}

fn build_thread_children_archived<T>(
    nodes: &ArchivedHashMap<u128_le, ArchivedIndependentNode<T>>,
    id: &u128_le,
    thread: &mut VecDeque<u128_le>,
) where
    T: IndependentContents + Archive,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_back(*id);

        for child in node.from.iter() {
            build_thread_children_archived(nodes, child, thread);
        }
    }
}

fn build_thread_parents_archived<T>(
    nodes: &ArchivedHashMap<u128_le, ArchivedIndependentNode<T>>,
    id: &u128_le,
    thread: &mut VecDeque<u128_le>,
) where
    T: IndependentContents + Archive,
{
    if let Some(node) = nodes.get(id)
        && node.active
    {
        thread.push_front(*id);

        for parent in node.to.iter() {
            build_thread_parents_archived(nodes, parent, thread);
        }
    }
}
