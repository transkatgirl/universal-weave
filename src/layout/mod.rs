//! [`Weave`] rendering helpers.
//!
//! This library provides 3 different 2D [`Layouter`] implementations with identical behavior:
//! - [`DependentLayouter`] - Takes a [`DependentWeave`] as an input.
//! - [`IndependentLayouter`] - Takes an [`IndependentWeave`] as an input.
//! - [`TopologicalLayouter`] - Takes any [`Weave`] as an input.

use core::hash::{BuildHasher, Hash};

use glam::Vec2;
use scratchpads::Scratchpad;

use crate::{
    IndependentContents, Layouter, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
};

/// Minimum gaps in a [`Weave`] layout.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[must_use]
pub struct Spacing {
    /// Gap between adjacent nodes.
    pub node: f32,
    /// Gap between layers of nodes.
    pub layer: f32,
    /// Gap between node connectors.
    pub connections: f32,
}

/// A 2D arrangement of a [`Weave`]'s contents.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Layout2D {
    // TODO
}

/// An error which occured while attempting to build a [`Layout2D`].
pub enum LayoutError {}

impl Default for Spacing {
    fn default() -> Self {
        Self {
            node: 16.0,
            layer: 16.0,
            connections: 8.0,
        }
    }
}

/// A 2D [`Layouter`] which takes a [`DependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct DependentLayouter {
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D,
}

impl DependentLayouter {
    /// Creates a new [`DependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<'a, K, T, M, S> Layouter<'a, DependentWeave<K, T, M, S>, K, DependentNode<K, T, S>, T>
    for DependentLayouter
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    type Size = Vec2;
    type Layout = &'a Layout2D;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut DependentWeave<K, T, M, S>,
        map: impl FnMut(&DependentNode<K, T, S>) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        todo!()
    }
}

/// A 2D [`Layouter`] which takes an [`IndependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct IndependentLayouter {
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D,
}

impl IndependentLayouter {
    /// Creates a new [`IndependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<'a, K, T, M, S> Layouter<'a, IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T>
    for IndependentLayouter
where
    K: Hash + Copy + Eq + Ord,
    T: IndependentContents,
    S: BuildHasher + Default + Clone,
{
    type Size = Vec2;
    type Layout = &'a Layout2D;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut IndependentWeave<K, T, M, S>,
        map: impl FnMut(&IndependentNode<K, T, S>) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        todo!()
    }
}

/// A 2D [`Layouter`] which orders nodes using [`Weave::get_ordered_identifiers()`].
///
/// However, this additional flexibility may result in worse performance and memory usage characteristics compared to [`DependentLayouter`] or [`IndependentLayouter`].
#[derive(Debug, Clone)]
#[must_use]
pub struct TopologicalLayouter {
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D,
    scratchpad: Scratchpad,
}

impl Default for TopologicalLayouter {
    fn default() -> Self {
        Self {
            spacing: Spacing::default(),
            layout: Layout2D::default(),
            scratchpad: Scratchpad::new(),
        }
    }
}

impl TopologicalLayouter {
    /// Creates a new [`TopologicalLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
            scratchpad: Scratchpad::new(),
        }
    }
}

impl<'a, W, K, N, T> Layouter<'a, W, K, N, T> for TopologicalLayouter
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
{
    type Size = Vec2;
    type Layout = &'a Layout2D;
    type Error = LayoutError;

    fn layout(
        &'a mut self,
        weave: &mut W,
        map: impl FnMut(&N) -> Self::Size,
    ) -> Result<Self::Layout, Self::Error> {
        todo!()
    }
}
