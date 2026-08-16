use core::hash::{BuildHasher, Hash};

use alloc::vec::Vec;
use glam::Vec2;
use hashbrown::HashMap;
use scratchpads::{Scratchpad, ScratchpadVec};

use crate::{
    IndependentContents, LayoutItem, Node, Weave, dependent::DependentWeave,
    independent::IndependentWeave, layout::Spacing,
};

#[derive(Debug, Clone)]
#[must_use]
pub struct Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    vertices: Vec<Vertex<K>>,
    top: Vec<usize>,
    bottom: Vec<usize>,
    reals_at: Vec<Vec<usize>>,
    seg_tops_at: Vec<Vec<usize>>,
    seg_bottoms_at: Vec<Vec<usize>>,
    merged_tops_at: Vec<Vec<usize>>,
    merged_bottoms_at: Vec<Vec<usize>>,
    up: Vec<Vec<(usize, usize)>>,
    down: Vec<Vec<(usize, usize)>>,
    edges: usize,
    height: usize,

    indices: HashMap<K, usize, S>,
    routes: HashMap<(K, K), Option<usize>, S>,
    coordinates: Vec<Vec2>,
    layer_y: Vec<f32>,
    layer_bounds: Vec<(f32, f32)>,
}

impl<K, S> Default for Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn default() -> Self {
        Self {
            vertices: Vec::new(),
            top: Vec::new(),
            bottom: Vec::new(),
            reals_at: Vec::new(),
            seg_tops_at: Vec::new(),
            seg_bottoms_at: Vec::new(),
            merged_tops_at: Vec::new(),
            merged_bottoms_at: Vec::new(),
            up: Vec::new(),
            down: Vec::new(),
            edges: 0,
            height: 0,

            indices: HashMap::default(),
            routes: HashMap::default(),
            coordinates: Vec::new(),
            layer_y: Vec::new(),
            layer_bounds: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum Vertex<K> {
    Real(K),
    Segment { from: K, to: K },
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn clear(&mut self, reserved_nodes: usize) {
        let ranks = self
            .bottom
            .iter()
            .max()
            .map_or(0, |&deepest| deepest.strict_add(1));
        let items = self.vertices.len();

        self.vertices.clear();
        self.vertices.reserve(reserved_nodes);
        self.top.clear();
        self.top.reserve(reserved_nodes);
        self.bottom.clear();
        self.bottom.reserve(reserved_nodes);

        for list in self
            .reals_at
            .iter_mut()
            .take(ranks)
            .chain(self.seg_tops_at.iter_mut().take(ranks))
            .chain(self.seg_bottoms_at.iter_mut().take(ranks))
            .chain(self.merged_tops_at.iter_mut().take(ranks))
            .chain(self.merged_bottoms_at.iter_mut().take(ranks))
        {
            list.clear();
        }

        for list in self
            .up
            .iter_mut()
            .take(items)
            .chain(self.down.iter_mut().take(items))
        {
            list.clear();
        }

        self.edges = 0;
        self.height = 0;

        self.indices.clear();
        self.indices.reserve(reserved_nodes);
        self.routes.clear();

        self.coordinates.clear();
        self.layer_y.clear();
        self.layer_bounds.clear();
    }
    fn push_item(&mut self, vertex: Vertex<K>, top: usize, bottom: usize) -> usize {
        todo!()
    }
    fn link(&mut self, from: usize, to: usize) {
        todo!()
    }
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    pub fn layout_dependent<T, M, F>(
        &mut self,
        weave: &mut DependentWeave<K, T, M, S>,
        sizes: F,
        spacing: &Spacing,
    ) where
        F: FnMut(&K) -> Vec2,
    {
        self.clear(weave.nodes.len());

        let guard = weave.scratchpad.guard();

        todo!()
    }
    pub fn layout_independent<T, M, F>(
        &mut self,
        weave: &mut IndependentWeave<K, T, M, S>,
        sizes: F,
        spacing: &Spacing,
    ) where
        T: IndependentContents,
        F: FnMut(&K) -> Vec2,
    {
        self.clear(weave.nodes.len());

        let guard = weave.scratchpad.guard();

        let mut stack = guard.vec();
        let mut identifier_map = guard.map_with_capacity(weave.nodes.len(), S::default());
        let mut parents: ScratchpadVec<'_, (K, usize, usize)> =
            guard.vec_with_capacity(weave.nodes.len());

        identifier_map.extend(weave.nodes.iter().map(|(&k, n)| (k, n.from.len())));

        stack.extend(weave.roots.iter().copied());

        while let Some(id) = stack.pop() {
            let node = &weave.nodes[&id];

            parents.extend(node.from.iter().map(|&id| {
                let index = self.indices[&id];
                (id, index, self.top[index])
            }));

            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            let rank = parents
                .iter()
                .map(|&(_, _, rank)| rank)
                .max()
                .map_or(0, |rank| rank + 1);

            let index = self.push_item(Vertex::Real(id), rank, rank);

            self.indices.insert(id, index);

            for (from, from_index, from_rank) in parents.drain(..) {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                if from_rank + 1 == rank {
                    self.link(from_index, index);
                    self.routes.insert((from, id), None);
                } else {
                    let segment = self.push_item(
                        Vertex::Segment { from, to: id },
                        from_rank.strict_add(1),
                        rank.strict_sub(1),
                    );

                    self.link(from_index, segment);
                    self.link(segment, index);
                    self.routes.insert((from, id), Some(segment));
                }
            }

            for child in node.to.iter().rev().copied() {
                let remaining = identifier_map.get_mut(&child).unwrap();
                #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                {
                    *remaining -= 1;
                }

                if *remaining == 0 {
                    stack.push(child);
                }
            }
        }

        debug_assert_eq!(weave.nodes.len(), self.indices.len(), "Malformed weave");

        todo!()
    }
    pub fn layout_topological<W, N, T, F>(
        &mut self,
        weave: &W,
        sizes: F,
        spacing: &Spacing,
        scratchpad: &mut Scratchpad,
        topological: &mut Vec<K>,
    ) where
        W: Weave<K, N, T>,
        K: Hash + Copy + Eq + Ord + 'static,
        N: Node<K, T>,
        F: FnMut(&K) -> Vec2,
        for<'a> &'a N::From: IntoIterator<Item = &'a K>,
    {
        self.clear(weave.len());

        let guard = scratchpad.guard();

        let mut parents: ScratchpadVec<'_, (K, usize, usize)> =
            guard.vec_with_capacity(weave.len());

        for id in topological.drain(..) {
            parents.extend(weave.get_parents(&id).unwrap().into_iter().map(|&id| {
                let index = self.indices[&id];
                (id, index, self.top[index])
            }));

            #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
            let rank = parents
                .iter()
                .map(|&(_, _, rank)| rank)
                .max()
                .map_or(0, |rank| rank + 1);

            let index = self.push_item(Vertex::Real(id), rank, rank);

            self.indices.insert(id, index);

            for (from, from_index, from_rank) in parents.drain(..) {
                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                if from_rank + 1 == rank {
                    self.link(from_index, index);
                    self.routes.insert((from, id), None);
                } else {
                    let segment = self.push_item(
                        Vertex::Segment { from, to: id },
                        from_rank.strict_add(1),
                        rank.strict_sub(1),
                    );

                    self.link(from_index, segment);
                    self.link(segment, index);
                    self.routes.insert((from, id), Some(segment));
                }
            }
        }

        assert_eq!(
            weave.len(),
            self.indices.len(),
            "Malformed topological order"
        );

        todo!()
    }
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    pub fn size(&self) -> Vec2 {
        todo!()
    }
    pub fn view<P, F>(&self, bounds: Vec2, callback: F)
    where
        P: Iterator<Item = Vec2>,
        F: FnMut(LayoutItem<K, Vec2, P>),
    {
        todo!()
    }
}
