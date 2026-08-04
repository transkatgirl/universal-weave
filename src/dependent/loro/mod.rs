//! [`loro`] wrapper for [`DependentWeave`].

use alloc::vec::Vec;
use core::{
    cmp::Ordering,
    hash::{BuildHasher, Hash},
};

use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use loro::{
    ExportMode, Frontiers, LoroDoc, LoroEncodeError, LoroTree, LoroValue, PeerID, TreeID,
    ValueOrContainer, VersionVector,
};
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
    ActiveSingularWeave, BookmarkableWeave, IndependentContents, MetadataWeave,
    SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave},
};

#[cfg(doc)]
use crate::{DiscreteWeave, Node};

/// A [`DependentWeave`] wrapper which adds collaborative editing using the [`loro`] CRDT library.
///
/// [`DiscreteWeave::split()`] and [`DiscreteWeave::merge_with_parent()`] are left intentionally unimplemented due to algorithmic limitations; Splitting/merging node contents must be done by adding a new [`Node`] with the updated contents to the [`Weave`].
///
/// It is strongly recommended that you make use of globally unique node identifiers (such as UUIDs) when using this wrapper to prevent node ID collisions.
///
/// # Conflict resolution
///
/// Conflicting [`SemiIndependentWeave::get_contents_mut`] updates and [`MetadataWeave::metadata_mut`] updates are currently handled via a Last Write Wins strategy. Additional conflict resolution strategies may be made available in the future.
///
/// # Synchronization
///
/// This wrapper attempts to keep state synchronized between a [`DependentWeave`] and [`LoroDoc`]. If this synchronization fails, this wrapper's [`Weave`] functions may create incorrect updates to the [`LoroDoc`], possibly resulting in panics.
///
/// Synchronization can be checked using [`DependentLoroWeave::validate()`].
///
/// [`DependentLoroWeave::update()`] provides the most straightforward route for resolving desynchronization. If `update()` fails, the [`LoroDoc`] needs to be manually modified before it can be used to update the [`DependentWeave`]'s state.
///
/// # Panics
///
/// The wrapper's [`Weave`] functions may panic if updating the inner [`LoroDoc`] fails or if the inner [`DependentWeave`] is internally inconsistent.
#[derive(Debug)]
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
    bookmark_mapping: Vec<usize>,
    tree_mapping: HashMap<K, TreeID, S>,
    scratchpad: Vec<(TreeID, Option<K>)>,
    buffer: AlignedVec,
    doc: LoroDoc,
}

impl<K, T, M, S> Clone for DependentLoroWeave<K, T, M, S>
where
    for<'a> K: Archive
        + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>
        + Hash
        + Copy
        + Eq
        + Ord,
    for<'a> K::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<K, Strategy<Pool, rancor::Error>>,
    for<'a> T:
        Clone + Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> T::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<T, Strategy<Pool, rancor::Error>>,
    for<'a> M:
        Clone + Archive + Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, rancor::Error>>,
    for<'a> M::Archived: CheckBytes<HighValidator<'a, rancor::Error>>
        + Deserialize<M, Strategy<Pool, rancor::Error>>,
    S: BuildHasher + Default + Clone,
{
    fn clone(&self) -> Self {
        Self {
            weave: self.weave.clone(),
            bookmark_mapping: self.bookmark_mapping.clone(),
            tree_mapping: self.tree_mapping.clone(),
            scratchpad: self.scratchpad.clone(),
            buffer: self.buffer.clone(),
            doc: self.doc.fork(),
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.weave.clone_from(&source.weave);
        self.bookmark_mapping.clone_from(&source.bookmark_mapping);
        self.tree_mapping.clone_from(&source.tree_mapping);
        self.scratchpad.clone_from(&source.scratchpad);
        self.buffer.clone_from(&source.buffer);
        self.doc = source.doc.fork();
    }
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
        value.get_ordered_identifiers(&mut self_nodes);

        let mut tree_mapping = HashMap::with_capacity_and_hasher(value.capacity(), S::default());

        for node in self_nodes {
            let node = value.get(&node).unwrap();

            let tree_id = tree
                .create(node.from.map(|id| tree_mapping.get(&id).copied().unwrap()))
                .unwrap();
            tree_mapping.insert(node.id, tree_id);

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

        let mut bookmark_mapping = Vec::with_capacity(value.capacity());

        for (index, bookmark) in value.bookmarked.iter().enumerate() {
            bookmark_mapping.push(index);
            bookmarks.push(to_bytes(bookmark)?.into_vec()).unwrap();
        }

        doc.commit();

        Ok(Self {
            doc,
            scratchpad: Vec::with_capacity(tree_mapping.capacity()),
            tree_mapping,
            bookmark_mapping,
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
            bookmark_mapping: Vec::with_capacity(weave.capacity()),
            tree_mapping: HashMap::with_capacity_and_hasher(weave.capacity(), S::default()),
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
    /// Returns `Err` if creating a [`LoroDoc`] from the weave's state fails.
    #[inline]
    pub fn from_weave(weave: DependentWeave<K, T, M, S>) -> Result<Self, rancor::Error> {
        Self::try_from(weave)
    }
    /// Creates a [`DependentLoroWeave`] from a [`LoroDoc`].
    ///
    /// # Errors
    ///
    /// Returns `Err` if creating a [`DependentWeave`] from the document fails.
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
    /// Update the weave's state by modifying the inner [`LoroDoc`].
    ///
    /// Attempting to modify the inner [`LoroDoc`] outside of this function using shallow cloning (such as [`LoroDoc::clone()`]) *will* lead to unexpected behavior, such as panics and/or data loss. However, since this function is fairly slow, it is highly recommended that you batch changes to the [`LoroDoc`] whenever possible.
    ///
    /// # Errors
    ///
    /// Returns `Err` if updating the weave's state from the inner [`LoroDoc`] fails.
    ///
    /// After an error occurs, the inner [`DependentWeave`] and [`LoroDoc`] will no longer be synchronized.
    ///
    /// # Panics
    ///
    /// May panic if `callback` panics.
    pub fn update<F, O>(&mut self, callback: F) -> Result<O, rancor::Error>
    where
        F: FnOnce(&mut LoroDoc) -> O,
    {
        let output = callback(&mut self.doc);
        match self.import() {
            Ok(()) => Ok(output),
            Err(error) => {
                self.scratchpad.clear();
                self.weave.clear();
                self.tree_mapping.clear();
                self.bookmark_mapping.clear();
                Err(error)
            }
        }
    }
    /// Returns the inner [`LoroDoc`]'s [`PeerID`].
    pub fn peer_id(&self) -> PeerID {
        self.doc.peer_id()
    }
    /// Returns the inner [`LoroDoc`]'s operation log [`VersionVector`].
    pub fn oplog_vv(&self) -> VersionVector {
        self.doc.oplog_vv()
    }
    /// Returns the inner [`LoroDoc`]'s operation log [`Frontiers`].
    pub fn oplog_frontiers(&self) -> Frontiers {
        self.doc.oplog_frontiers()
    }
    /// Exports the inner [`LoroDoc`]'s state.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the inner [`LoroDoc::export()`] fails.
    pub fn export(&mut self, mode: ExportMode) -> Result<Vec<u8>, LoroEncodeError> {
        self.doc.export(mode)
    }
    #[allow(clippy::panic_in_result_fn, reason = "Upholding internal invariant")]
    fn import(&mut self) -> Result<(), rancor::Error> {
        self.tree_mapping.clear();
        self.bookmark_mapping.clear();
        self.weave.clear();

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if self.doc.is_detached() {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Document must not be detached".into(),
            )))?;
        }

        if !tree.is_fractional_index_enabled() {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Fractional index must be enabled".into(),
            )))?;
        }

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

        assert!(self.weave.nodes.len() < usize::MAX - 1, "Too many nodes");

        if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
            metadata.get("active_node")
        {
            let active = from_bytes_aligned(&binary, &mut self.buffer)?;

            if let Some(active) = active {
                self.weave.set_active(&active, true);
            } else {
                self.weave.active = None;
            }
        } else {
            Err(rancor::Error::new(loro::LoroError::Unknown(
                "Malformed active status".into(),
            )))?;
        }

        for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate() {
            if let LoroValue::Binary(binary) = bookmark {
                let bookmark = from_bytes_aligned(&binary, &mut self.buffer)?;

                if !self.weave.contains_bookmark(&bookmark)
                    && self.weave.set_bookmarked(&bookmark, true)
                {
                    self.bookmark_mapping.push(index);
                }
            } else {
                Err(rancor::Error::new(loro::LoroError::Unknown(
                    "Malformed bookmark".into(),
                )))?;
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
                if self.weave.insert(DependentNode {
                    id,
                    from: parent,
                    to: IndexSet::default(),
                    active: false,
                    bookmarked: false,
                    contents: from_bytes_aligned(&binary_contents, &mut self.buffer)?,
                }) {
                    self.tree_mapping.insert(id, target);

                    if let Some(children) = tree.children(target) {
                        self.scratchpad
                            .extend(children.into_iter().rev().map(|child| (child, Some(id))));
                    }
                } else {
                    Err(rancor::Error::new(loro::LoroError::Unknown(
                        "Invalid node".into(),
                    )))?;
                }
            } else {
                Err(rancor::Error::new(loro::LoroError::Unknown(
                    "Malformed node".into(),
                )))?;
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
    fn get(&self, id: &K) -> Option<&DependentNode<K, T, S>> {
        self.weave.get(id)
    }
    #[inline]
    fn get_parents(&self, id: &K) -> Option<&Option<K>> {
        self.weave.get_parents(id)
    }
    #[inline]
    fn get_children(&self, id: &K) -> Option<&IndexSet<K, S>> {
        self.weave.get_children(id)
    }
    #[inline]
    fn get_contents(&self, id: &K) -> Option<&T> {
        self.weave.get_contents(id)
    }
    #[inline]
    fn get_ordered_identifiers(&mut self, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers(output);
    }
    #[inline]
    fn get_ordered_identifiers_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_ordered_identifiers_from(id, output);
    }
    #[inline]
    fn get_active_path(&mut self, output: &mut Vec<K>) {
        self.weave.get_active_path(output);
    }
    #[inline]
    fn get_path_from(&mut self, id: &K, output: &mut Vec<K>) {
        self.weave.get_path_from(id, output);
    }
    fn insert(&mut self, node: DependentNode<K, T, S>) -> bool {
        let id = node.id;
        let from = node.from;
        let mut active = node.active;
        let bookmarked = node.bookmarked;
        let contents = to_bytes(&node.contents).unwrap();

        if self.weave.insert(node) {
            assert!(self.weave.nodes.len() < usize::MAX - 1, "Too many nodes");

            let id_bytes = to_bytes(&id).unwrap().into_vec();

            let tree = self.doc.get_tree("tree");
            let metadata = self.doc.get_map("metadata");
            let bookmarks = self.doc.get_movable_list("bookmarks");

            let tree_id = tree
                .create(from.map(|id| self.tree_mapping.get(&id).copied().unwrap()))
                .unwrap();
            self.tree_mapping.insert(id, tree_id);

            let meta = tree.get_meta(tree_id).unwrap();
            meta.insert("id", id_bytes.clone()).unwrap();
            meta.insert("contents", contents.into_vec()).unwrap();

            let mut was_dangling = false;

            for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate().rev() {
                if let LoroValue::Binary(binary) = bookmark
                    && from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap() == id
                {
                    bookmarks.delete(index, 1).unwrap();
                    was_dangling = true;
                }
            }

            if bookmarked {
                bookmarks.push(id_bytes).unwrap();
            }

            if bookmarked || was_dangling {
                self.bookmark_mapping.clear();
                self.bookmark_mapping
                    .resize(self.weave.bookmarked.len(), usize::MAX);

                for (index, value) in bookmarks.to_vec().into_iter().enumerate() {
                    if let LoroValue::Binary(binary) = value
                        && let Some(pos) = self.weave.bookmarked.get_index_of(
                            &from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap(),
                        )
                        && self.bookmark_mapping[pos] == usize::MAX
                    {
                        self.bookmark_mapping[pos] = index;
                    }
                }
            }

            if let Some(ValueOrContainer::Value(LoroValue::Binary(binary))) =
                metadata.get("active_node")
                && from_bytes_aligned::<Option<K>, _>(&binary, &mut self.buffer).unwrap()
                    == Some(id)
            {
                active = true;
            }

            if active {
                metadata
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
    fn set_active(&mut self, id: &K, value: bool) -> bool {
        if self.weave.set_active(id, value) {
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
    fn remove(&mut self, id: &K) -> Option<DependentNode<K, T, S>> {
        let old_bookmarks: Option<HashSet<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        let mut removed_node = None;

        if self.weave.remove_tracked(id, |node| {
            if &node.id == id {
                removed_node = Some(node);
            } else {
                self.tree_mapping.remove(&node.id).unwrap();
            }
        }) {
            self.doc
                .get_tree("tree")
                .delete(self.tree_mapping.remove(id).unwrap())
                .unwrap();

            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();

            let bookmarks = self.doc.get_movable_list("bookmarks");

            let mut removed_bookmarks = old_bookmarks.unwrap();
            removed_bookmarks.retain(|id| !self.weave.bookmarked.contains(id));

            for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate().rev() {
                if let LoroValue::Binary(binary) = bookmark
                    && removed_bookmarks
                        .contains(&from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap())
                {
                    bookmarks.delete(index, 1).unwrap();
                }
            }

            self.bookmark_mapping.clear();
            self.bookmark_mapping
                .resize(self.weave.bookmarked.len(), usize::MAX);

            for (index, value) in bookmarks.to_vec().into_iter().enumerate() {
                if let LoroValue::Binary(binary) = value
                    && let Some(pos) = self.weave.bookmarked.get_index_of(
                        &from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap(),
                    )
                    && self.bookmark_mapping[pos] == usize::MAX
                {
                    self.bookmark_mapping[pos] = index;
                }
            }
        }

        removed_node
    }
    fn remove_tracked(
        &mut self,
        id: &K,
        mut on_removal: impl FnMut(DependentNode<K, T, S>),
    ) -> bool {
        let old_bookmarks: Option<HashSet<K>> = if self.weave.contains(id) {
            Some(self.weave.bookmarked.iter().copied().collect())
        } else {
            None
        };

        if self.weave.remove_tracked(id, |node| {
            if &node.id != id {
                self.tree_mapping.remove(&node.id).unwrap();
            }
            on_removal(node);
        }) {
            self.doc
                .get_tree("tree")
                .delete(self.tree_mapping.remove(id).unwrap())
                .unwrap();

            self.doc
                .get_map("metadata")
                .insert(
                    "active_node",
                    to_bytes(&self.weave.active).unwrap().into_vec(),
                )
                .unwrap();

            let bookmarks = self.doc.get_movable_list("bookmarks");

            let mut removed_bookmarks = old_bookmarks.unwrap();
            removed_bookmarks.retain(|id| !self.weave.bookmarked.contains(id));

            for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate().rev() {
                if let LoroValue::Binary(binary) = bookmark
                    && removed_bookmarks
                        .contains(&from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap())
                {
                    bookmarks.delete(index, 1).unwrap();
                }
            }

            self.bookmark_mapping.clear();
            self.bookmark_mapping
                .resize(self.weave.bookmarked.len(), usize::MAX);

            for (index, value) in bookmarks.to_vec().into_iter().enumerate() {
                if let LoroValue::Binary(binary) = value
                    && let Some(pos) = self.weave.bookmarked.get_index_of(
                        &from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap(),
                    )
                    && self.bookmark_mapping[pos] == usize::MAX
                {
                    self.bookmark_mapping[pos] = index;
                }
            }

            true
        } else {
            false
        }
    }
    fn clear(&mut self) {
        self.weave.clear();
        self.tree_mapping.clear();
        self.bookmark_mapping.clear();

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
    /// Validates that the state of the inner [`LoroDoc`] and [`DependentWeave`] are synchronized enough for [`Weave`] operations to function properly.
    #[allow(clippy::too_many_lines, reason = "Marginally exceeds threshold")]
    pub fn validate(&self) -> bool {
        let mut buffer = AlignedVec::with_capacity(self.buffer.capacity());

        let tree = self.doc.get_tree("tree");
        let metadata = self.doc.get_map("metadata");
        let bookmarks = self.doc.get_movable_list("bookmarks");

        if self.doc.is_detached() || !tree.is_fractional_index_enabled() {
            return false;
        }

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
            && (self.weave.active == active
                || active.is_some_and(|active| {
                    !self.weave.nodes.contains_key(&active) && self.weave.active.is_none()
                }))
        {
        } else {
            return false;
        }

        let bookmarks = bookmarks.to_vec();

        if self.bookmark_mapping.len() != self.weave.bookmarked.len() {
            return false;
        }

        for (weave_index, loro_index) in self.bookmark_mapping.iter().copied().enumerate() {
            if let Some(LoroValue::Binary(binary)) = bookmarks.get(loro_index)
                && let Ok(bookmark) = from_bytes_aligned(binary, &mut buffer)
                && self.weave.bookmarked.get_index(weave_index) == Some(&bookmark)
            {
            } else {
                return false;
            }
        }

        let mut counter: usize = 0;

        for (index, bookmark) in bookmarks.into_iter().enumerate() {
            if counter < self.bookmark_mapping.len() && self.bookmark_mapping[counter] == index {
                counter = counter.strict_add(1);
            } else if let LoroValue::Binary(binary) = bookmark
                && let Ok(bookmark) = from_bytes_aligned::<K, _>(&binary, &mut buffer)
            {
                if let Some(pos) = self.weave.bookmarked.get_index_of(&bookmark) {
                    if self.bookmark_mapping[pos] > index {
                        return false;
                    }
                } else if self.weave.nodes.contains_key(&bookmark) {
                    return false;
                }
            } else {
                return false;
            }
        }

        if counter != self.bookmark_mapping.len() {
            return false;
        }

        let mut counter: usize = 0;

        for (index, root) in tree.roots().into_iter().enumerate() {
            if let Some(at_index) = self.weave.roots.get_index(index)
                && self.tree_mapping.get(at_index) == Some(&root)
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
                        && let Some(node) = self.weave.get(&id)
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
                                && self.tree_mapping.get(at_index) == Some(child)
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
    fn set_bookmarked(&mut self, id: &K, value: bool) -> bool {
        let was_bookmarked = self.weave.contains_bookmark(id);

        if self.weave.set_bookmarked(id, value) {
            if value != was_bookmarked {
                let bookmarks = self.doc.get_movable_list("bookmarks");

                if value {
                    bookmarks.push(to_bytes(id).unwrap().into_vec()).unwrap();
                } else {
                    for (index, bookmark) in bookmarks.to_vec().into_iter().enumerate().rev() {
                        if let LoroValue::Binary(binary) = bookmark
                            && from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap() == *id
                        {
                            bookmarks.delete(index, 1).unwrap();
                        }
                    }
                }

                self.bookmark_mapping.clear();
                self.bookmark_mapping
                    .resize(self.weave.bookmarked.len(), usize::MAX);

                for (index, value) in bookmarks.to_vec().into_iter().enumerate() {
                    if let LoroValue::Binary(binary) = value
                        && let Some(pos) = self.weave.bookmarked.get_index_of(
                            &from_bytes_aligned::<K, _>(&binary, &mut self.buffer).unwrap(),
                        )
                        && self.bookmark_mapping[pos] == usize::MAX
                    {
                        self.bookmark_mapping[pos] = index;
                    }
                }
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
    fn sort_children_by(
        &mut self,
        id: &K,
        cmp: impl FnMut(&DependentNode<K, T, S>, &DependentNode<K, T, S>) -> Ordering,
    ) -> bool {
        if self.weave.sort_children_by(id, cmp) {
            let tree = self.doc.get_tree("tree");
            let parent = self.tree_mapping.get(id).copied().unwrap();

            for (index, child) in self.weave.get(id).unwrap().to.iter().enumerate() {
                tree.mov_to(
                    self.tree_mapping.get(child).copied().unwrap(),
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
    fn sort_children_by_id(&mut self, id: &K, cmp: impl FnMut(&K, &K) -> Ordering) -> bool {
        if self.weave.sort_children_by_id(id, cmp) {
            let tree = self.doc.get_tree("tree");
            let parent = self.tree_mapping.get(id).copied().unwrap();

            for (index, child) in self.weave.get(id).unwrap().to.iter().enumerate() {
                tree.mov_to(
                    self.tree_mapping.get(child).copied().unwrap(),
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
            tree.mov_to(self.tree_mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
    fn sort_roots_by_id(&mut self, cmp: impl FnMut(&K, &K) -> Ordering) {
        self.weave.sort_roots_by_id(cmp);

        let tree = self.doc.get_tree("tree");

        for (index, root) in self.weave.roots.iter().enumerate() {
            tree.mov_to(self.tree_mapping.get(root).copied().unwrap(), None, index)
                .unwrap();
        }
    }
}

// TODO: Find a way to swap Loro items so that reordering will no longer be O(N^2)
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
                bookmarks
                    .mov(
                        self.bookmark_mapping[old_index],
                        self.bookmark_mapping[index],
                    )
                    .unwrap();
                old_bookmarks.move_index(old_index, index);
                self.bookmark_mapping[old_index] = self.bookmark_mapping[index];
                for mid in &mut self.bookmark_mapping[index..old_index] {
                    *mid = mid.strict_add(1);
                }
                self.bookmark_mapping[index..=old_index].rotate_right(1);
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
                bookmarks
                    .mov(
                        self.bookmark_mapping[old_index],
                        self.bookmark_mapping[index],
                    )
                    .unwrap();
                old_bookmarks.move_index(old_index, index);
                self.bookmark_mapping[old_index] = self.bookmark_mapping[index];
                for mid in &mut self.bookmark_mapping[index..old_index] {
                    *mid = mid.strict_add(1);
                }
                self.bookmark_mapping[index..=old_index].rotate_right(1);
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
                .get_meta(self.tree_mapping.get(id).copied().unwrap())
                .unwrap()
                .insert("contents", to_bytes(contents).unwrap().into_vec())
                .unwrap();

            output
        })
    }
}
