//! [`Weave`] rendering helpers.
//!
//! This library provides 3 different 2D [`Layouter`] implementations with identical behavior:
//! - [`DependentLayouter`] - Takes a [`DependentWeave`] as an input.
//! - [`IndependentLayouter`] - Takes an [`IndependentWeave`] as an input.
//! - [`TopologicalLayouter`] - Takes any [`Weave`] as an input.

use core::{
    hash::{BuildHasher, Hash},
    num::FpCategory,
};

use alloc::vec::Vec;
use glam::Vec2;
use scratchpads::Scratchpad;

use crate::{
    IndependentContents, LayoutItem, Layouter, Node, Weave,
    dependent::{DependentNode, DependentWeave},
    independent::{IndependentNode, IndependentWeave},
    layout::positioner::Layout2D,
};

/*

Tests to write:
- DependentLayouter parity with TopologicalLayouter
- IndependentLayouter parity with TopologicalLayouter
- TopologicalLayouter property testing w/ function contracts

Need to add curve fitting convenience functions

*/

mod positioner;
mod slotset;

/// Minimum gaps in a [`Weave`] layout.
///
/// All values must be normal numbers >= 0.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
#[must_use]
pub struct Spacing {
    /// Gap between adjacent nodes.
    pub node: f32,
    /// Gap between layers of nodes.
    pub layer: f32,
    /// Gap between edges.
    pub corridor: f32,
    /// Gap between an edge and a node.
    pub edge: f32,
}

impl Default for Spacing {
    fn default() -> Self {
        Self {
            node: 16.0,
            layer: 16.0,
            corridor: 8.0,
            edge: 4.0,
        }
    }
}

impl Spacing {
    /// Validates that all spacing values are normal numbers >= 0.
    #[must_use]
    pub const fn validate(&self) -> bool {
        validate_float(self.node)
            && validate_float(self.layer)
            && validate_float(self.corridor)
            && validate_float(self.edge)
    }
}

/// A 2D [`Layouter`] which takes a [`DependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
}

impl<K, S> DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`DependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<K, T, M, S> Layouter<DependentWeave<K, T, M, S>, K, DependentNode<K, T, S>, T, Vec2>
    for DependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut DependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        self.layout.layout_dependent(weave, sizes, &self.spacing);
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<'a>(&'a mut self, min: Vec2, max: Vec2, callback: impl FnMut(LayoutItem<'a, K, Vec2>)) {
        self.layout.view(min, max, callback);
    }
}

/// A 2D [`Layouter`] which takes an [`IndependentWeave`] as input.
///
/// This layout algorithm has identical behavior to [`TopologicalLayouter`].
#[derive(Default, Debug, Clone)]
#[must_use]
pub struct IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
}

impl<K, S> IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`IndependentLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
        }
    }
}

impl<K, T, M, S> Layouter<IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T, Vec2>
    for IndependentLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord + 'static,
    T: IndependentContents,
    S: BuildHasher + Default + Clone + 'static,
{
    fn layout(&mut self, weave: &mut IndependentWeave<K, T, M, S>, sizes: impl FnMut(&K) -> Vec2) {
        self.layout.layout_independent(weave, sizes, &self.spacing);
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<'a>(&'a mut self, min: Vec2, max: Vec2, callback: impl FnMut(LayoutItem<'a, K, Vec2>)) {
        self.layout.view(min, max, callback);
    }
}

/// A 2D [`Layouter`] which orders nodes using [`Weave::get_ordered_identifiers()`].
///
/// However, this additional flexibility may result in worse performance and memory usage characteristics compared to [`DependentLayouter`] or [`IndependentLayouter`].
#[derive(Debug, Clone)]
#[must_use]
pub struct TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// The [`Spacing`] used to arrange contents.
    pub spacing: Spacing,

    layout: Layout2D<K, S>,
    topological: Vec<K>,
    scratchpad: Scratchpad,
}

impl<K, S> Default for TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self::new(Spacing::default())
    }
}

impl<K, S> TopologicalLayouter<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    /// Creates a new [`TopologicalLayouter`] with the specified spacing.
    pub fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            layout: Layout2D::default(),
            topological: Vec::new(),
            scratchpad: Scratchpad::new(),
        }
    }
}

impl<W, K, N, T, S> Layouter<W, K, N, T, Vec2> for TopologicalLayouter<K, S>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord + 'static,
    N: Node<K, T>,
    S: BuildHasher + Default + Clone + 'static,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
{
    fn layout(&mut self, weave: &mut W, sizes: impl FnMut(&K) -> Vec2) {
        weave.get_ordered_identifiers(&mut self.topological);

        assert_eq!(
            weave.len(),
            self.topological.len(),
            "Malformed topological order"
        );

        self.layout.layout_topological(
            weave,
            sizes,
            &self.spacing,
            &mut self.scratchpad,
            &mut self.topological,
        );
    }
    fn size(&self) -> Vec2 {
        self.layout.size()
    }
    fn view<'a>(&'a mut self, min: Vec2, max: Vec2, callback: impl FnMut(LayoutItem<'a, K, Vec2>)) {
        self.layout.view(min, max, callback);
    }
}

#[must_use]
const fn validate_float(value: f32) -> bool {
    matches!(
        value.classify(),
        FpCategory::Normal | FpCategory::Zero | FpCategory::Subnormal
    ) && value.is_sign_positive()
}

#[must_use]
const fn validate_vec2(value: Vec2) -> bool {
    validate_float(value.x) && validate_float(value.y)
}
