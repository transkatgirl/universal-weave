//! [`loro`] wrapper for [`DependentWeave`] (WIP)

use std::{
    collections::HashMap,
    hash::{BuildHasher, Hash},
};

use indexmap::IndexSet;
use loro::{LoroDoc, LoroTree, LoroValue, TreeID, ValueOrContainer};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::Pool,
    from_bytes,
    rancor::{self, Source, Strategy},
    ser::allocator::ArenaHandle,
    to_bytes,
    util::AlignedVec,
};

use crate::{
    ActiveSingularWeave, DeduplicatableContents, DeduplicatableWeave, IndependentContents,
    SemiIndependentWeave, Weave,
    dependent::{DependentNode, DependentWeave},
};

/// A [`DependentWeave`] wrapper which adds collaborative editing using [`loro`].
pub struct DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    weave: DependentWeave<K, T, M, S>,
    mapping: HashMap<K, TreeID, S>,
    doc: LoroDoc,
}

impl<K, T, M, S> AsRef<DependentWeave<K, T, M, S>> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn as_ref(&self) -> &DependentWeave<K, T, M, S> {
        &self.weave
    }
}

impl<K, T, M, S> From<DependentLoroWeave<K, T, M, S>> for DependentWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn from(value: DependentLoroWeave<K, T, M, S>) -> Self {
        value.weave
    }
}

impl<K, T, M, S> TryFrom<DependentWeave<K, T, M, S>> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Error = rancor::Error;

    fn try_from(mut value: DependentWeave<K, T, M, S>) -> Result<Self, Self::Error> {
        let doc = LoroDoc::new();
        let tree = doc.get_tree("tree");
        let metadata = doc.get_map("metadata");
        let bookmarks = doc.get_movable_list("bookmarks");

        let mut self_nodes = Vec::with_capacity(value.len());
        value.get_ordered_node_identifiers(&mut self_nodes);

        let mut mapping: HashMap<K, TreeID, S> =
            HashMap::with_capacity_and_hasher(value.len(), S::default());

        for node in self_nodes {
            let node = value.get_node(&node).unwrap();

            let tree_id = tree
                .create(node.from.map(|id| mapping.get(&id).copied().unwrap()))
                .unwrap();
            mapping.insert(node.id, tree_id);

            let meta = tree.get_meta(tree_id).unwrap();
            meta.insert("id", to_bytes(&node.id)?.into_vec()).unwrap();
            meta.insert("contents", to_bytes(&node.contents)?.into_vec())
                .unwrap();
        }

        metadata
            .insert("active_node", to_bytes(&value.active)?.into_vec())
            .unwrap();
        metadata
            .insert("contents", to_bytes(&value.metadata)?.into_vec())
            .unwrap();

        for bookmark in &value.bookmarked {
            bookmarks.push(to_bytes(bookmark)?.into_vec()).unwrap();
        }

        doc.commit();

        Ok(Self {
            doc,
            mapping,
            weave: value,
        })
    }
}

impl<K, T, M, S> TryFrom<LoroDoc> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Error = rancor::Error;

    fn try_from(value: LoroDoc) -> Result<Self, Self::Error> {
        let tree = value.get_tree("tree");
        let metadata = value.get_map("metadata");

        let metadata = if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("contents")
        {
            from_bytes_aligned(&binary)?
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed metadata".into(),
            )))?
        };

        let weave: DependentWeave<K, T, M, S> =
            DependentWeave::with_capacity(tree.nodes().len(), metadata);

        let mut wrapped = Self {
            mapping: HashMap::with_capacity_and_hasher(weave.capacity(), S::default()),
            weave,
            doc: value,
        };

        wrapped.import()?;

        Ok(wrapped)
    }
}

impl<K, T, M, S> DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    pub fn new(weave: DependentWeave<K, T, M, S>) -> Result<Self, rancor::Error> {
        Self::try_from(weave)
    }
    pub fn into_weave(self) -> DependentWeave<K, T, M, S> {
        self.weave
    }
    pub fn metadata(&self) -> &M {
        &self.weave.metadata
    }
    pub fn set_metadata(&mut self, metadata: M) {
        self.weave.metadata = metadata;
        self.doc
            .get_map("metadata")
            .insert(
                "contents",
                to_bytes(&self.weave.metadata).unwrap().into_vec(),
            )
            .unwrap();
    }
    /// Update the weave's state by modifying the corresponding [`LoroDoc`]
    ///
    /// Attempting to clone the inner [`LoroDoc`] and modify it outside of this function *will* lead to unexpected behavior, including but not limited to panics and data loss.
    ///
    /// This function is farly slow, so it is highly recommended that you batch changes to the [`LoroDoc`] whenever possible.
    pub fn update(&mut self, callback: impl FnOnce(&mut LoroDoc)) -> Result<(), rancor::Error> {
        callback(&mut self.doc);
        match self.import() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.weave.remove_all_nodes();
                self.mapping.clear();
                Err(error)
            }
        }
    }
    fn import(&mut self) -> Result<(), rancor::Error> {
        self.mapping.clear();

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) = metadata.get("contents") {
            self.weave.metadata = from_bytes_aligned(&binary)?;
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed metadata".into(),
            )))?
        }

        self.weave.remove_all_nodes();

        for root in tree.roots() {
            self.import_subtree(&tree, root, None)?;
        }

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("active_node")
        {
            self.weave
                .set_node_active_status_in_place(&from_bytes_aligned(&binary)?, true);
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed node".into(),
            )))?
        }

        for bookmark in bookmarks.to_vec() {
            if let LoroValue::Binary(binary) = bookmark {
                self.weave
                    .set_node_bookmarked_status(&from_bytes_aligned(&binary)?, true);
            } else {
                Err(rancor::Error::new(loro::LoroError::Unknown(
                    "Malformed bookmark".into(),
                )))?
            }
        }

        Ok(())
    }
    fn import_subtree(
        &mut self,
        tree: &LoroTree,
        target: TreeID,
        parent: Option<K>,
    ) -> Result<(), rancor::Error> {
        let meta = tree.get_meta(target).map_err(rancor::Error::new)?;

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary_id))) = meta.get("id")
            && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_contents))) =
                meta.get("contents")
        {
            let id = from_bytes_aligned(&binary_id)?;
            self.mapping.insert(id, target);

            if self.weave.add_node(DependentNode {
                id,
                from: parent,
                to: IndexSet::default(),
                active: false,
                bookmarked: false,
                contents: from_bytes_aligned(&binary_contents)?,
            }) && let Some(children) = tree.children(target)
            {
                for child in children {
                    self.import_subtree(tree, child, Some(id))?;
                }
            }
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed node".into(),
            )))?
        };

        Ok(())
    }
}

fn from_bytes_aligned<T, E>(bytes: &[u8]) -> Result<T, E>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<HighValidator<'a, E>> + Deserialize<T, Strategy<Pool, E>>,
    E: Source,
{
    let mut aligned = AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    from_bytes(&aligned)
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, DependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;
    type Bookmarks = IndexSet<K, S>;

    fn len(&self) -> usize {
        self.weave.len()
    }
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.weave.get_node(id)
    }
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_node_identifiers(output);
    }
    fn get_active_thread(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_thread(output);
    }
    fn get_thread_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_thread_from(id, output);
    }
    fn add_node(&mut self, node: DependentNode<K, T, S>) -> bool {
        let id = node.id;
        let from = node.from;
        let active = node.active;
        let bookmarked = node.bookmarked;
        let contents = to_bytes(&node.contents).unwrap();

        if self.weave.add_node(node) {
            let id_bytes = to_bytes(&id).unwrap().into_vec();

            let tree = self.doc.get_tree("tree");

            let tree_id = tree
                .create(from.map(|id| self.mapping.get(&id).copied().unwrap()))
                .unwrap();
            self.mapping.insert(id, tree_id);

            let meta = tree.get_meta(tree_id).unwrap();
            meta.insert("id", id_bytes.clone()).unwrap();
            meta.insert("contents", contents.into_vec()).unwrap();

            if bookmarked {
                self.doc
                    .get_movable_list("bookmarks")
                    .push(id_bytes.clone())
                    .unwrap();
            }

            if active {
                self.doc
                    .get_map("metadata")
                    .insert("active_node", id_bytes)
                    .unwrap();
            }

            true
        } else {
            false
        }
    }
    fn set_node_active_status(&mut self, id: &K, value: bool, alternate: bool) -> bool {
        if self.weave.set_node_active_status(id, value, alternate) {
            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();
            true
        } else {
            false
        }
    }
    fn set_node_active_status_in_place(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_active_status_in_place(id, value) {
            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();
            true
        } else {
            false
        }
    }
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
        let bookmark_index = if !value {
            self.weave.bookmarked.get_index_of(id)
        } else {
            None
        };

        if self.weave.set_node_bookmarked_status(id, value) {
            if value {
                self.doc
                    .get_movable_list("bookmarks")
                    .push(to_bytes(id).unwrap().into_vec())
                    .unwrap();
            } else {
                self.doc
                    .get_movable_list("bookmarks")
                    .delete(bookmark_index.unwrap(), 1)
                    .unwrap();
            }

            true
        } else {
            false
        }
    }
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let old_bookmarks: Option<Vec<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        if let Some(node) = self.weave.remove_node(id) {
            self.mapping.remove(&node.id);

            self.doc
                .get_tree("tree")
                .delete(self.mapping.get(id).copied().unwrap())
                .unwrap();

            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();

            let mut offset: usize = 0;
            let bookmarks = self.doc.get_movable_list("bookmarks");

            for (index, bookmark) in old_bookmarks.unwrap().into_iter().enumerate() {
                if !self.weave.bookmarked.contains(&bookmark) {
                    bookmarks.delete(index - offset, 1).unwrap();
                    offset += 1;
                }
            }

            Some(node)
        } else {
            None
        }
    }
    fn remove_all_nodes(&mut self) {
        self.weave.remove_all_nodes();
        self.mapping.clear();

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        for root in tree.roots() {
            tree.delete(root).unwrap();
        }

        metadata
            .insert("active_node", to_bytes(&None::<K>).unwrap().into_vec())
            .unwrap();

        bookmarks.clear().unwrap();
    }
}

/*impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        self.weave
            .get_ordered_node_identifiers_reversed_children(output);
    }
    fn sort_node_children_by(
        &mut self,
        id: &K,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> std::cmp::Ordering,
    ) -> bool {
        todo!()
    }
    fn sort_node_children_by_id(
        &mut self,
        id: &K,
        cmp: impl FnMut(&K, &K) -> std::cmp::Ordering,
    ) -> bool {
        todo!()
    }
    fn sort_roots_by(
        &mut self,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> std::cmp::Ordering,
    ) {
        todo!()
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> std::cmp::Ordering) {
        todo!()
    }
    fn sort_bookmarks_by(
        &mut self,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> std::cmp::Ordering,
    ) {
        todo!()
    }
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> std::cmp::Ordering) {
        todo!()
    }
}*/

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

/*impl<K, T, M, S> DiscreteWeave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + DiscreteContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn split_node(&mut self, id: &K, at: usize, new_id: K) -> bool {
        todo!()
    }
    fn merge_with_parent(&mut self, id: &K) -> Option<K> {
        todo!()
    }
}*/

impl<K, T, M, S> DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + IndependentContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    /// Replacement for [`SemiIndependentWeave::get_contents_mut()`]
    pub fn set_contents<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        if let Some(contents) = self.weave.get_contents_mut(id) {
            let output = callback(contents);

            let meta = self
                .doc
                .get_tree("tree")
                .get_meta(self.mapping.get(id).copied().unwrap())
                .unwrap();
            meta.insert("contents", to_bytes(contents).unwrap().into_vec())
                .unwrap();

            Some(output)
        } else {
            None
        }
    }
}

impl<K, T, M, S> DeduplicatableWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + DeduplicatableContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.weave.find_duplicates(id)
    }
}
