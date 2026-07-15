use std::hash::{BuildHasher, Hash};

use loro::{LoroTree, LoroValue, TreeID, ValueOrContainer};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighSerializer, HighValidator},
    bytecheck::CheckBytes,
    de::Pool,
    rancor::{self, Strategy},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};
use stacksafe::stacksafe;

#[allow(unused_imports)]
use loro::LoroDoc;

use crate::{
    Weave,
    dependent::loro::{DependentLoroWeave, from_bytes_aligned},
};

#[allow(unused_imports)]
use crate::dependent::DependentWeave;

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
    #[must_use]
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
