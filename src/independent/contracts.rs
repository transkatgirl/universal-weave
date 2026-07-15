use std::{
    collections::HashSet,
    hash::{BuildHasher, Hash},
};

use indexmap::IndexSet;
use stacksafe::stacksafe;

#[allow(unused_imports)]
use crate::Weave;

use crate::{IndependentContents, independent::IndependentWeave};

impl<K, T, M, S> IndependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    ///
    /// If this returns `false`, further actions on the weave will result in unexpected behavior, including but not limited to panics. However, since this function is fairly slow, it should only be called occasionally (such as when saving the weave to disk).
    ///
    /// This function will be removed in the future once this [`Weave`] implementation has undergone formal verification.
    #[must_use]
    pub fn validate(&self) -> bool {
        let nodes: IndexSet<_, _> = self.nodes.keys().copied().collect();
        let nodes_std: HashSet<_, _> = self.nodes.keys().copied().collect();
        let active_index: IndexSet<_, _> = self.active.iter().copied().collect();

        self.roots.is_subset::<S>(&nodes)
            && self.validate_active()
            && self.active.is_subset(&nodes_std)
            && self.bookmarked.is_subset::<S>(&nodes)
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
                        !value.from.is_disjoint::<S>(&active_index)
                    } else {
                        true
                    }
            })
    }
    fn validate_active(&self) -> bool {
        let mut threads = Vec::new();

        for active_root in self.roots.iter().filter(|root| self.active.contains(root)) {
            threads.push(Vec::new());
            let index = threads.len() - 1;
            if !self.build_path(active_root, &mut threads, index) {
                return false;
            }
        }

        let mut longest = (0, 0);

        for (index, thread) in threads.iter().enumerate() {
            if thread.len() > longest.0 {
                longest = (thread.len(), index);
            }
        }

        if threads.is_empty() {
            return self.active.is_empty();
        }

        let thread = threads.swap_remove(longest.1);

        thread.len() == self.active.len() && HashSet::from_iter(thread).is_subset(&self.active)
    }
    #[stacksafe]
    fn build_path(&self, node: &K, threads: &mut Vec<Vec<K>>, index: usize) -> bool {
        if let Some(node) = self.nodes.get(node) {
            threads[index].push(node.id);

            for active_child in node.to.iter().filter(|root| self.active.contains(root)) {
                threads.push(threads[index].clone());
                if !self.build_path(active_child, threads, threads.len() - 1) {
                    return false;
                }
            }

            true
        } else {
            false
        }
    }
    #[must_use]
    pub(super) fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
}
