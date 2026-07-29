//! [`loro`] wrapper for [`DependentWeave`].

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
};

use hashbrown::HashMap;
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
    ActiveSingularWeave, BookmarkableWeave, DeduplicatableContents, DeduplicatableWeave,
    IndependentContents, MetadataWeave, SemiIndependentWeave, SortableBookmarkableWeave,
    SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
};

#[cfg(doc)]
use crate::{DiscreteWeave, Node};

/// A [`DependentWeave`] wrapper which adds collaborative editing using the [`loro`] CRDT library.
///
/// [`DiscreteWeave::split_node()`] and [`DiscreteWeave::merge_with_parent()`] are left intentionally unimplemented due to algorithmic limitations; Splitting/merging node contents must be done by adding a new [`Node`] with the updated contents to the [`Weave`].
///
/// It is strongly recommended that you make use of globally unique node identifiers (such as UUIDs) if you plan on using this wrapper.
///
/// # Panics
///
/// The wrapper's [`Weave`] functions may panic if updating the underlying [`LoroDoc`] fails or if the underlying [`DependentWeave`] is internally inconsistent.
#[derive(Debug, Clone)]
#[must_use]
pub struct DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    weave: DependentWeave<K, T, M, S>,
    mapping: HashMap<K, TreeID, S>,
    scratchpad: Vec<(TreeID, Option<K>)>,
    buffer: AlignedVec,
    doc: LoroDoc,
}

impl<K, T, M, S> AsRef<DependentWeave<K, T, M, S>> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
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
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn from(value: DependentLoroWeave<K, T, M, S>) -> Self {
        value.weave
    }
}

impl<K, T, M, S> From<DependentLoroWeave<K, T, M, S>> for LoroDoc
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn from(value: DependentLoroWeave<K, T, M, S>) -> Self {
        value.doc
    }
}

impl<K, T, M, S> TryFrom<DependentWeave<K, T, M, S>> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
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

        tree.enable_fractional_index(0);

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
            scratchpad: Vec::with_capacity(mapping.len()),
            mapping,
            buffer: AlignedVec::with_capacity(4096),
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
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Error = rancor::Error;

    fn try_from(value: LoroDoc) -> Result<Self, Self::Error> {
        let tree = value.get_tree("tree");
        let metadata = value.get_map("metadata");

        let (metadata, buffer) = if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("contents")
        {
            let mut buffer = AlignedVec::with_capacity(binary.len().max(4096));
            buffer.extend_from_slice(&binary);

            (from_bytes(&buffer)?, buffer)
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed metadata".into(),
            )))?
        };

        let weave: DependentWeave<K, T, M, S> =
            DependentWeave::with_capacity(tree.nodes().len(), metadata);

        let mut wrapped = Self {
            mapping: HashMap::with_capacity_and_hasher(weave.capacity(), S::default()),
            scratchpad: Vec::with_capacity(weave.capacity()),
            buffer,
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
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    /// Creates a [`DependentLoroWeave`] from a [`DependentWeave`].
    ///
    /// # Errors
    ///
    /// Will return `Err` if creating a [`LoroDoc`] from the weave's state fails.
    #[inline]
    pub fn from_weave(weave: DependentWeave<K, T, M, S>) -> Result<Self, rancor::Error> {
        Self::try_from(weave)
    }
    /// Creates a [`DependentLoroWeave`] from a [`LoroDoc`].
    ///
    /// # Errors
    ///
    /// Will return `Err` if creating a [`DependentWeave`] from the document fails.
    #[inline]
    pub fn from_doc(doc: LoroDoc) -> Result<Self, rancor::Error> {
        Self::try_from(doc)
    }
    /// Converts a [`DependentLoroWeave`] into a [`DependentWeave`].
    #[inline]
    pub fn into_weave(self) -> DependentWeave<K, T, M, S> {
        self.weave
    }
    /// Returns a reference to the inner [`DependentWeave`].
    #[inline]
    pub const fn as_weave(&self) -> &DependentWeave<K, T, M, S> {
        &self.weave
    }
    /// Converts a [`DependentLoroWeave`] into a [`LoroDoc`].
    #[inline]
    #[must_use]
    pub fn into_doc(self) -> LoroDoc {
        self.doc
    }
    /// Update the weave's state by modifying the corresponding [`LoroDoc`].
    ///
    /// Attempting to modify the inner [`LoroDoc`] outside of this function using shallow cloning (such as [`LoroDoc::clone()`]) *will* lead to unexpected behavior, such as panics and/or data loss. However, since this function is farly slow, it is highly recommended that you batch changes to the [`LoroDoc`] whenever possible.
    ///
    /// This function does not squash generated [`LoroDoc`] operations that cancel out.
    ///
    /// # Errors
    ///
    /// Will return `Err` if updating the weave's state from the corresponding [`LoroDoc`] fails.
    ///
    /// If an error occurs, all nodes will be removed from the weave.
    ///
    /// # Panics
    ///
    /// May panic if `callback` panics.
    pub fn update<F>(&mut self, callback: F) -> Result<(), rancor::Error>
    where
        F: FnOnce(&mut LoroDoc),
    {
        callback(&mut self.doc);
        match self.import() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.scratchpad.clear();
                self.weave.remove_all_nodes();
                self.mapping.clear();
                Err(error)
            }
        }
    }
    fn import(&mut self) -> Result<(), rancor::Error> {
        self.mapping.clear();
        self.weave.remove_all_nodes();

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) = metadata.get("contents") {
            self.weave.metadata = from_bytes_aligned(&binary, &mut self.buffer)?;
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed metadata".into(),
            )))?;
        }

        for root in tree.roots() {
            self.import_subtree(&tree, root, None)?;
        }

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("active_node")
        {
            let active = from_bytes_aligned(&binary, &mut self.buffer)?;

            if let Some(active) = active {
                if !self.weave.set_node_active_status(&active, true) {
                    metadata
                        .insert("active_node", to_bytes(&None::<K>)?.into_vec())
                        .map_err(rancor::Error::new)?;
                }
            } else {
                self.weave.active = None;
            }
        } else {
            metadata
                .insert("active_node", to_bytes(&None::<K>)?.into_vec())
                .map_err(rancor::Error::new)?;
        }

        let mut offset = 0;

        for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate() {
            if let LoroValue::Binary(binary) = bookmark {
                let bookmark = from_bytes_aligned(&binary, &mut self.buffer)?;

                if self.weave.contains_bookmark(&bookmark)
                    || !self.weave.set_node_bookmarked_status(&bookmark, true)
                {
                    bookmarks
                        .delete(index.strict_sub(offset), 1)
                        .map_err(rancor::Error::new)?;
                    offset = offset.strict_add(1);
                }
            } else {
                bookmarks
                    .delete(index.strict_sub(offset), 1)
                    .map_err(rancor::Error::new)?;
                offset = offset.strict_add(1);
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
        self.scratchpad.push((target, parent));

        while let Some((target, parent)) = self.scratchpad.pop() {
            let meta = tree.get_meta(target).map_err(rancor::Error::new)?;

            if let Some(ValueOrContainer::Value(LoroValue::Binary(binary_id))) = meta.get("id")
                && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_contents))) =
                    meta.get("contents")
            {
                let id = from_bytes_aligned(&binary_id, &mut self.buffer)?;
                if self.weave.add_node(DependentNode {
                    id,
                    from: parent,
                    to: IndexSet::default(),
                    active: false,
                    bookmarked: false,
                    contents: from_bytes_aligned(&binary_contents, &mut self.buffer)?,
                }) {
                    self.mapping.insert(id, target);

                    if let Some(children) = tree.children(target) {
                        self.scratchpad
                            .extend(children.into_iter().rev().map(|child| (child, Some(id))));
                    }
                } else {
                    tree.delete(target).map_err(rancor::Error::new)?;
                }
            } else {
                tree.delete(target).map_err(rancor::Error::new)?;
            }
        }

        Ok(())
    }
}

fn from_bytes_aligned<T, E>(bytes: &[u8], buffer: &mut AlignedVec) -> Result<T, E>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<HighValidator<'a, E>> + Deserialize<T, Strategy<Pool, E>>,
    E: Source,
{
    buffer.clear();
    buffer.extend_from_slice(bytes);
    from_bytes(buffer)
}

impl<K, T, M, S> Weave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Nodes = HashMap<K, DependentNode<K, T, S>, S>;
    type Roots = IndexSet<K, S>;

    #[inline]
    fn len(&self) -> usize {
        self.weave.len()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.weave.is_empty()
    }
    #[inline]
    fn nodes(&self) -> &Self::Nodes {
        self.weave.nodes()
    }
    #[inline]
    fn roots(&self) -> &Self::Roots {
        self.weave.roots()
    }
    #[inline]
    fn contains(&self, id: &K) -> bool {
        self.weave.contains(id)
    }
    #[inline]
    fn contains_active(&self, id: &K) -> bool {
        self.weave.contains_active(id)
    }
    #[inline]
    fn get_node(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.weave.get_node(id)
    }
    #[inline]
    fn get_ordered_node_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_node_identifiers(output);
    }
    #[inline]
    fn get_ordered_node_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_node_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
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
                    .push(id_bytes)
                    .unwrap();
            }

            if active {
                self.doc
                    .get_map("metadata")
                    .insert(
                        "active_node",
                        to_bytes(&self.weave.active).unwrap().into_vec(),
                    )
                    .unwrap();
            }

            true
        } else {
            false
        }
    }
    fn set_node_active_status(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_node_active_status(id, value) {
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
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let old_bookmarks: Option<Vec<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        let mut removed_node = None;

        if self.weave.remove_node_tracked(id, |node| {
            if &node.id == id {
                removed_node = Some(node);
            } else {
                self.mapping.remove(&node.id).unwrap();
            }
        }) {
            self.doc
                .get_tree("tree")
                .delete(self.mapping.remove(id).unwrap())
                .unwrap();

            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();

            let bookmarks = self.doc.get_movable_list("bookmarks");

            for (index, bookmark) in old_bookmarks.unwrap().into_iter().enumerate().rev() {
                if !self.weave.bookmarked.contains(&bookmark) {
                    bookmarks.delete(index, 1).unwrap();
                }
            }
        }

        removed_node
    }
    fn remove_node_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        let old_bookmarks: Option<Vec<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        if self.weave.remove_node_tracked(id, |node| {
            if &node.id != id {
                self.mapping.remove(&node.id).unwrap();
            }
            on_removal(node);
        }) {
            self.doc
                .get_tree("tree")
                .delete(self.mapping.remove(id).unwrap())
                .unwrap();

            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();

            let bookmarks = self.doc.get_movable_list("bookmarks");

            for (index, bookmark) in old_bookmarks.unwrap().into_iter().enumerate().rev() {
                if !self.weave.bookmarked.contains(&bookmark) {
                    bookmarks.delete(index, 1).unwrap();
                }
            }

            true
        } else {
            false
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

impl<K, T, M, S> DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>> + Eq,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>> + Eq,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the internal [`LoroDoc`] is consistent with the [`DependentWeave`]'s state.
    pub fn validate(&self) -> bool {
        let mut buffer = AlignedVec::with_capacity(self.buffer.capacity());

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) = metadata.get("contents")
            && let Ok(metadata) = from_bytes_aligned(&binary, &mut buffer)
            && self.weave.metadata == metadata
        {
        } else {
            return false;
        }

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("active_node")
            && let Ok(active) = from_bytes_aligned(&binary, &mut buffer)
            && self.weave.active == active
        {
        } else {
            return false;
        }

        let bookmarks = bookmarks.to_vec();

        if self.weave.bookmarked.len() != bookmarks.len() {
            return false;
        }

        for (index, bookmark) in bookmarks.into_iter().enumerate() {
            if let LoroValue::Binary(binary) = bookmark
                && let Ok(bookmark) = from_bytes_aligned(&binary, &mut buffer)
                && self.weave.bookmarked.get_index(index) == Some(&bookmark)
            {
            } else {
                return false;
            }
        }

        let mut counter: usize = 0;

        for (index, root) in tree.roots().into_iter().enumerate() {
            if let Some(at_index) = self.weave.roots.get_index(index)
                && self.mapping.get(at_index) == Some(&root)
            {
                let mut stack = Vec::with_capacity(self.weave.len());

                stack.push((root, None));

                while let Some((target, parent)) = stack.pop() {
                    if let Ok(meta) = tree.get_meta(target)
                        && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_id))) =
                            meta.get("id")
                        && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_contents))) =
                            meta.get("contents")
                        && let Ok(id) = from_bytes_aligned(&binary_id, &mut buffer)
                        && let Ok(contents) = from_bytes_aligned(&binary_contents, &mut buffer)
                        && let Some(node) = self.weave.get_node(&id)
                        && node.from == parent
                        && node.contents == contents
                    {
                        counter = counter.strict_add(1);

                        let children = tree.children(target).unwrap_or_default();

                        if node.to.len() != children.len() {
                            return false;
                        }

                        for (index, child) in children.iter().enumerate() {
                            if let Some(at_index) = node.to.get_index(index)
                                && self.mapping.get(at_index) == Some(child)
                            {
                            } else {
                                return false;
                            }
                        }

                        stack.extend(children.into_iter().rev().map(|child| (child, Some(id))));
                    } else {
                        return false;
                    }
                }
            } else {
                return false;
            }
        }

        if counter != self.weave.nodes.len() {
            return false;
        }

        self.weave.validate()
    }
}

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn metadata(&self) -> &M {
        &self.weave.metadata
    }
    fn metadata_mut<O>(&mut self, callback: impl FnOnce(&mut M) -> O) -> O {
        self.weave.metadata_mut(|metadata| {
            let output = callback(metadata);

            self.doc
                .get_map("metadata")
                .insert("contents", to_bytes(metadata).unwrap().into_vec())
                .unwrap();

            output
        })
    }
}

impl<K, T, M, S> BookmarkableWeave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    type Bookmarks = IndexSet<K, S>;

    #[inline]
    fn bookmarks(&self) -> &Self::Bookmarks {
        self.weave.bookmarks()
    }
    #[inline]
    fn contains_bookmark(&self, id: &K) -> bool {
        self.weave.contains_bookmark(id)
    }
    fn set_node_bookmarked_status(&mut self, id: &K, value: bool) -> bool {
        let bookmark_index = self.weave.bookmarked.get_index_of(id);

        if self.weave.set_node_bookmarked_status(id, value) {
            if value && bookmark_index.is_none() {
                self.doc
                    .get_movable_list("bookmarks")
                    .push(to_bytes(id).unwrap().into_vec())
                    .unwrap();
            } else if !value && let Some(bookmark_index) = bookmark_index {
                self.doc
                    .get_movable_list("bookmarks")
                    .delete(bookmark_index, 1)
                    .unwrap();
            }

            true
        } else {
            false
        }
    }
}

// TODO: Find a way to swap Loro items so that reordering will no longer be O(N^2)
impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn get_ordered_node_identifiers_reversed_children(&mut self, output: &mut Vec<K>) {
        self.weave
            .get_ordered_node_identifiers_reversed_children(output);
    }
    #[inline]
    fn get_ordered_node_identifiers_from_reversed_children(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave
            .get_ordered_node_identifiers_from_reversed_children(id, output);
    }
    fn sort_node_children_by(
        &mut self,
        id: &K,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if self.weave.sort_node_children_by(id, cmp) {
            let tree = self.doc.get_tree("tree");
            let parent = self.mapping.get(id).copied().unwrap();

            for (index, child) in self.weave.get_node(id).unwrap().to.iter().enumerate() {
                tree.mov_to(
                    self.mapping.get(child).copied().unwrap(),
                    Some(parent),
                    index,
                )
                .unwrap();
            }

            true
        } else {
            false
        }
    }
    fn sort_node_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_node_children_by_id(id, cmp) {
            let tree = self.doc.get_tree("tree");
            let parent = self.mapping.get(id).copied().unwrap();

            for (index, child) in self.weave.get_node(id).unwrap().to.iter().enumerate() {
                tree.mov_to(
                    self.mapping.get(child).copied().unwrap(),
                    Some(parent),
                    index,
                )
                .unwrap();
            }

            true
        } else {
            false
        }
    }
    fn sort_roots_by(
        &mut self,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        self.weave.sort_roots_by(cmp);

        let tree = self.doc.get_tree("tree");

        for (index, root) in self.weave.roots.iter().enumerate() {
            tree.mov_to(self.mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);

        let tree = self.doc.get_tree("tree");

        for (index, root) in self.weave.roots.iter().enumerate() {
            tree.mov_to(self.mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
}

impl<K, T, M, S> SortableBookmarkableWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn sort_bookmarks_by(
        &mut self,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) {
        let bookmarks = self.doc.get_movable_list("bookmarks");

        let mut old_bookmarks = self.weave.bookmarked.clone();
        self.weave.sort_bookmarks_by(cmp);

        for (index, bookmark) in self.weave.bookmarked.iter().enumerate() {
            let old_index = old_bookmarks.get_index_of(bookmark).unwrap();

            if index != old_index {
                bookmarks.mov(old_index, index).unwrap();
                old_bookmarks.move_index(old_index, index);
            }
        }
    }
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        let bookmarks = self.doc.get_movable_list("bookmarks");

        let mut old_bookmarks = self.weave.bookmarked.clone();
        self.weave.sort_bookmarks_by_id(cmp);

        for (index, bookmark) in self.weave.bookmarked.iter().enumerate() {
            let old_index = old_bookmarks.get_index_of(bookmark).unwrap();

            if index != old_index {
                bookmarks.mov(old_index, index).unwrap();
                old_bookmarks.move_index(old_index, index);
            }
        }
    }
}

impl<K, T, M, S> ActiveSingularWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn active(&self) -> Option<K> {
        self.weave.active()
    }
}

impl<K, T, M, S> SemiIndependentWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + IndependentContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn get_contents_mut<O>(&mut self, id: &K, callback: impl FnOnce(&mut T) -> O) -> Option<O> {
        self.weave.get_contents_mut(id, |contents| {
            let output = callback(contents);

            self.doc
                .get_tree("tree")
                .get_meta(self.mapping.get(id).copied().unwrap())
                .unwrap()
                .insert("contents", to_bytes(contents).unwrap().into_vec())
                .unwrap();

            output
        })
    }
}

impl<K, T, M, S> DeduplicatableWeave<K, DependentNode<K, T, S>, T>
    for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + DeduplicatableContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    #[inline]
    fn find_duplicates(&self, id: &K) -> impl Iterator<Item = K> {
        self.weave.find_duplicates(id)
    }
}
