//! [`loro`] wrapper for [`DependentWeave`]

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
use stacksafe::stacksafe;

use crate::{
    ActiveSingularWeave, DeduplicatableContents, DeduplicatableWeave, IndependentContents,
    SemiIndependentWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
};

#[allow(unused)]
use crate::{DiscreteWeave, MetadataWeave, Node};

/// A [`DependentWeave`] wrapper which adds collaborative editing using [`loro`].
///
/// [`DiscreteWeave::split_node()`] and [`DiscreteWeave::merge_with_parent()`] are left intentionally unimplemented due to algorithmic limitations; Splitting/merging node contents must be done by adding a new [`Node`] with the updated contents to the [`Weave`].
///
/// It is strongly recommended that you make use of globally unique node identifiers (such as UUIDs) if you plan on using this wrapper.
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
    buffer: AlignedVec,
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

impl<K, T, M, S> From<DependentLoroWeave<K, T, M, S>> for LoroDoc
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
        value.doc
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

        tree.enable_fractional_index(1);

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
    pub fn into_doc(self) -> LoroDoc {
        self.doc
    }
    /// Update the weave's state by modifying the corresponding [`LoroDoc`].
    ///
    /// Attempting to modify the inner [`LoroDoc`] outside of this function using shallow cloning (such as [`LoroDoc::clone()`]) *will* lead to unexpected behavior, such as panics and/or data loss. However, since this function is farly slow, it is highly recommended that you batch changes to the [`LoroDoc`] whenever possible.
    ///
    /// This function does not squash generated [`LoroDoc`] operations that cancel out.
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
            self.weave.metadata = from_bytes_aligned(&binary, &mut self.buffer)?;
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
            && self.weave.set_node_active_status_in_place(
                &from_bytes_aligned(&binary, &mut self.buffer)?,
                true,
            )
        {
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
                        .delete(index - offset, 1)
                        .map_err(rancor::Error::new)?;
                    offset += 1;
                }
            } else {
                bookmarks
                    .delete(index - offset, 1)
                    .map_err(rancor::Error::new)?;
                offset += 1;
            }
        }

        Ok(())
    }
    #[stacksafe]
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
                    for child in children {
                        self.import_subtree(tree, child, Some(id))?;
                    }
                }
            } else {
                tree.delete(target).map_err(rancor::Error::new)?;
            }
        } else {
            tree.delete(target).map_err(rancor::Error::new)?;
        };

        Ok(())
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
    for<'a> T: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>> + Eq,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
    for<'a> M: Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>> + Eq,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the internal [`LoroDoc`] is consistent with the [`DependentWeave`]'s state.
    ///
    /// If this returns `false`, further actions will result in unexpected behavior, including but not limited to panics. However, since this function is fairly slow, it should only be called occasionally.
    pub fn validate(&mut self) -> bool {
        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) = metadata.get("contents")
            && let Ok(metadata) = from_bytes_aligned(&binary, &mut self.buffer)
            && self.weave.metadata == metadata
        {
        } else {
            return false;
        }

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("active_node")
            && let Ok(active) = from_bytes_aligned(&binary, &mut self.buffer)
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
                && let Ok(bookmark) = from_bytes_aligned(&binary, &mut self.buffer)
                && self.weave.bookmarked.get_index(index) == Some(&bookmark)
            {
            } else {
                return false;
            }
        }

        let mut counter = 0;

        for (index, root) in tree.roots().into_iter().enumerate() {
            if let Some(at_index) = self.weave.roots.get_index(index)
                && self.mapping.get(at_index) == Some(&root)
                && self.validate_subtree(&tree, root, None, &mut counter)
            {
            } else {
                return false;
            }
        }

        if counter != self.weave.len() {
            return false;
        }

        true
    }
    #[stacksafe]
    fn validate_subtree(
        &mut self,
        tree: &LoroTree,
        target: TreeID,
        parent: Option<K>,
        counter: &mut usize,
    ) -> bool {
        if let Ok(meta) = tree.get_meta(target)
            && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_id))) = meta.get("id")
            && let Some(ValueOrContainer::Value(LoroValue::Binary(binary_contents))) =
                meta.get("contents")
            && let Ok(id) = from_bytes_aligned(&binary_id, &mut self.buffer)
            && let Ok(contents) = from_bytes_aligned(&binary_contents, &mut self.buffer)
            && let Some(node) = self.weave.get_node(&id)
            && node.from == parent
            && node.contents == contents
        {
            *counter += 1;

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

            for child in children {
                if !self.validate_subtree(tree, child, Some(id), counter) {
                    return false;
                }
            }

            true
        } else {
            false
        }
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
    fn remove_node(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let old_bookmarks: Option<Vec<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        if let Some(node) = self.weave.remove_node(id) {
            self.doc
                .get_tree("tree")
                .delete(self.mapping.remove(&node.id).unwrap())
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

impl<K, T, M, S> MetadataWeave<K, DependentNode<K, T, S>, T, M> for DependentLoroWeave<K, T, M, S>
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

// TODO: Find a way to swap Loro items so that reordering will no longer be O(N^2)
impl<K, T, M, S> SortableWeave<K, DependentNode<K, T, S>, T> for DependentLoroWeave<K, T, M, S>
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
    fn sort_node_children_by_id(
        &mut self,
        id: &K,
        cmp: impl FnMut(&K, &K) -> std::cmp::Ordering,
    ) -> bool {
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
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> std::cmp::Ordering,
    ) {
        self.weave.sort_roots_by(cmp);

        let tree = self.doc.get_tree("tree");

        for (index, root) in self.weave.roots.iter().enumerate() {
            tree.mov_to(self.mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> std::cmp::Ordering) {
        self.weave.sort_roots_by_id(cmp);

        let tree = self.doc.get_tree("tree");

        for (index, root) in self.weave.roots.iter().enumerate() {
            tree.mov_to(self.mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
    fn sort_bookmarks_by(
        &mut self,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> std::cmp::Ordering,
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
    fn sort_bookmarks_by_id(&mut self, cmp: impl FnMut(&K, &K) -> std::cmp::Ordering) {
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

impl<K, T, M, S> SemiIndependentWeave<K, DependentNode<K, T, S>, T>
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
        + IndependentContents,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    M: Archive,
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
