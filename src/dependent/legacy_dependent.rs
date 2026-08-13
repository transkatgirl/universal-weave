//! A legacy version of `DependentWeave` used by `tapestry-weave`'s v0 format.

use alloc::vec::Vec;
use core::hash::{BuildHasher, Hash};

use hashbrown::HashMap;
use indexmap::IndexSet;
use scratchpads::Scratchpad;

#[cfg(feature = "rkyv")]
use hashbrown::HashSet;

#[cfg(feature = "rkyv")]
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::Verify,
    rancor::{Fallible, Source, fail},
    with::Skip,
};

#[cfg(feature = "serde")]
use serde::{
    Deserialize as SerdeDeserialize, Deserializer as SerdeDeserializer,
    Serialize as SerdeSerialize, de::Error as _,
};

use crate::dependent::{DependentNode, DependentWeave as NewDependentWeave, detect_cycles};

#[cfg(feature = "rkyv")]
use crate::dependent::{
    ArchivedDependentWeave as NewArchivedDependentWeave, archived_detect_cycles,
};

#[cfg(any(feature = "serde", feature = "rkyv"))]
use crate::contract::ValidationError;

#[cfg(doc)]
use crate::Node;

impl<K, T, M, S> From<DependentWeave<K, T, M, S>> for NewDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn from(value: DependentWeave<K, T, M, S>) -> Self {
        Self {
            nodes: value.nodes,
            roots: value.roots,
            active: value.active,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            metadata: value.metadata,
        }
    }
}

impl<K, T, M, S> From<NewDependentWeave<K, T, M, S>> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn from(value: NewDependentWeave<K, T, M, S>) -> Self {
        Self {
            nodes: value.nodes,
            roots: value.roots,
            active: value.active,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            thread: Vec::new(),
            metadata: value.metadata,
        }
    }
}

/// A legacy version of [`super::DependentWeave`] used by `tapestry-weave`'s v0 format.
///
/// This type can be converted to/from [`super::DependentWeave`] with zero overhead.
#[derive(Default, Debug, Clone)]
#[cfg_attr(feature = "rkyv", derive(Archive, Deserialize, Serialize))]
#[cfg_attr(feature = "serde", derive(SerdeSerialize))]
#[cfg_attr(feature = "rkyv", rkyv(bytecheck(verify)))]
#[must_use]
pub struct DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
            deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
        ))
    )]
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    roots: IndexSet<K, S>,
    active: Option<K>,
    #[cfg_attr(
        feature = "serde",
        serde(bound(
            serialize = "IndexSet<K, S>: SerdeSerialize",
            deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
        ))
    )]
    bookmarked: IndexSet<K, S>,

    // Legacy field required for deserialization
    #[allow(dead_code)]
    thread: Vec<K>,

    #[cfg_attr(feature = "rkyv", rkyv(with = Skip))]
    #[cfg_attr(feature = "serde", serde(skip))]
    scratchpad: Scratchpad,

    /// The metadata associated with the weave.
    pub metadata: M,
}

#[cfg(feature = "serde")]
#[derive(SerdeDeserialize)]
#[serde(rename = "DependentWeave")]
struct ProxyDependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[serde(bound(
        serialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeSerialize",
        deserialize = "HashMap<K, DependentNode<K, T, S>, S>: SerdeDeserialize<'de>"
    ))]
    nodes: HashMap<K, DependentNode<K, T, S>, S>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    roots: IndexSet<K, S>,
    active: Option<K>,
    #[serde(bound(
        serialize = "IndexSet<K, S>: SerdeSerialize",
        deserialize = "IndexSet<K, S>: SerdeDeserialize<'de>"
    ))]
    bookmarked: IndexSet<K, S>,
    thread: Vec<K>,
    metadata: M,
}

#[cfg(feature = "serde")]
#[allow(clippy::missing_trait_methods, reason = "Conflicting lint")]
impl<'de, K, T, M, S> SerdeDeserialize<'de> for DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord + SerdeDeserialize<'de>,
    T: SerdeDeserialize<'de>,
    M: SerdeDeserialize<'de>,
    S: BuildHasher + Default + Clone,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: SerdeDeserializer<'de>,
    {
        let proxy = ProxyDependentWeave::deserialize(deserializer)?;
        let weave = Self {
            nodes: proxy.nodes,
            roots: proxy.roots,
            active: proxy.active,
            bookmarked: proxy.bookmarked,
            thread: proxy.thread,
            scratchpad: Scratchpad::new(),
            metadata: proxy.metadata,
        };

        if weave.validate() {
            Ok(weave)
        } else {
            Err(D::Error::custom(ValidationError))
        }
    }
}

impl<K, T, M, S> DependentWeave<K, T, M, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Validates that the weave is internally consistent.
    pub fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .as_ref()
                .is_none_or(|active| self.nodes.contains_key(active))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .as_ref()
                        .is_none_or(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value.to.iter().all(|v| {
                        self.nodes
                            .get(v)
                            .is_some_and(|p| p.from.as_ref() == Some(key))
                    })
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && !detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut Vec::with_capacity(self.roots.len()),
            )
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> From<ArchivedDependentWeave<K, T, M, S>> for NewArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn from(value: ArchivedDependentWeave<K, T, M, S>) -> Self {
        Self {
            nodes: value.nodes,
            roots: value.roots,
            active: value.active,
            bookmarked: value.bookmarked,
            scratchpad: value.scratchpad,
            metadata: value.metadata,
        }
    }
}

#[cfg(feature = "rkyv")]
impl<K, T, M, S> ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
{
    fn validate(&self) -> bool {
        self.roots
            .iter()
            .all(move |value| self.nodes.contains_key(value))
            && self
                .active
                .as_ref()
                .is_none_or(|active| self.nodes.contains_key(active))
            && self
                .bookmarked
                .iter()
                .all(move |value| self.nodes.contains_key(value))
            && self.nodes.iter().all(|(key, value)| {
                value.validate()
                    && value.id == *key
                    && value
                        .from
                        .as_ref()
                        .is_none_or(|v| self.nodes.get(v).is_some_and(|p| p.to.contains(key)))
                    && value.to.iter().all(|v| {
                        self.nodes
                            .get(v)
                            .is_some_and(|p| p.from.as_ref() == Some(key))
                    })
                    && value.from.is_none() == self.roots.contains(key)
                    && value.active == (self.active == Some(*key))
                    && value.bookmarked == self.bookmarked.contains(key)
            })
            && !archived_detect_cycles(
                &self.nodes,
                self.roots.iter().copied(),
                &mut Vec::with_capacity(self.roots.len()),
                &mut HashSet::with_capacity(self.nodes.len()),
            )
    }
}

#[cfg(feature = "rkyv")]
// SAFETY:
// All fields are safe to access and no unsafe functions are called
unsafe impl<K, T, M, S, C> Verify<C> for ArchivedDependentWeave<K, T, M, S>
where
    K: Archive + Hash + Copy + Eq + Ord,
    <K as Archive>::Archived: Hash + Copy + Eq + Ord + 'static,
    T: Archive,
    M: Archive,
    S: BuildHasher + Default + Clone,
    C: Fallible + ?Sized,
    C::Error: Source,
{
    fn verify(&self, _context: &mut C) -> Result<(), C::Error> {
        if !self.validate() {
            fail!(ValidationError)
        }

        Ok(())
    }
}
