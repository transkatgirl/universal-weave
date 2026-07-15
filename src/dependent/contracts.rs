use std::hash::{BuildHasher, Hash};

use indexmap::IndexSet;

use crate::dependent::DependentWeave;

#[allow(unused_imports)]
use crate::Weave;

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq,
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

        self.roots.is_subset::<S>(&nodes)
            && if let Some(active) = self.active {
                self.nodes.contains_key(&active)
            } else {
                true
            }
            && self.bookmarked.is_subset(&nodes)
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
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
    #[must_use]
    pub(super) fn under_max_size(&self) -> bool {
        (self.nodes.len() as u64) < (i32::MAX as u64)
    }
}
