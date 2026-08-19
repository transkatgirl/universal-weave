#![allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::many_single_char_names,
    reason = "WIP"
)]

// TODO: Substantial clean-up work, further optimizations

use core::{
    hash::{BuildHasher, Hash},
    iter,
};

use alloc::vec::Vec;
use glam::Vec2;
use hashbrown::HashMap;
use scratchpads::{Scratchpad, ScratchpadVec};

use crate::{
    IndependentContents, LayoutItem, Node, Weave,
    dependent::DependentWeave,
    independent::IndependentWeave,
    layout::{Spacing, slotset::SlotSet, validate_float, validate_vec2},
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
    real_offsets: Vec<usize>,
    real_flat: Vec<usize>,
    seg_top_offsets: Vec<usize>,
    seg_top_flat: Vec<usize>,
    seg_bottom_offsets: Vec<usize>,
    seg_bottom_flat: Vec<usize>,
    merged_top_offsets: Vec<usize>,
    merged_top_flat: Vec<usize>,
    merged_bottom_offsets: Vec<usize>,
    merged_bottom_flat: Vec<usize>,
    up_offsets: Vec<usize>,
    up_flat: Vec<(usize, usize)>,
    down_offsets: Vec<usize>,
    down_flat: Vec<(usize, usize)>,
    edge_list: Vec<(usize, usize)>,
    height: usize,
    indices: HashMap<K, usize, S>,
    sizes: Vec<Vec2>,
    rank_half_width: Vec<f32>,
    coordinates: Vec<Vec2>,
    layer_y: Vec<f32>,
    layer_bounds: Vec<(f32, f32)>,
    deepest: Vec<usize>,
    size: Vec2,
    polyline_points: Vec<Vec2>,
    polylines: Vec<Vec<Polyline<K>>>,
    polyline_bounds: Vec<(Vec2, Vec2)>,
    polyline_reach: Vec<(f32, f32)>,
    reach_prefix: Vec<f32>,
    rank_built: Vec<bool>,
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
            real_offsets: Vec::new(),
            real_flat: Vec::new(),
            seg_top_offsets: Vec::new(),
            seg_top_flat: Vec::new(),
            seg_bottom_offsets: Vec::new(),
            seg_bottom_flat: Vec::new(),
            merged_top_offsets: Vec::new(),
            merged_top_flat: Vec::new(),
            merged_bottom_offsets: Vec::new(),
            merged_bottom_flat: Vec::new(),
            up_offsets: Vec::new(),
            up_flat: Vec::new(),
            down_offsets: Vec::new(),
            down_flat: Vec::new(),
            edge_list: Vec::new(),
            height: 0,
            indices: HashMap::default(),
            sizes: Vec::new(),
            rank_half_width: Vec::new(),
            coordinates: Vec::new(),
            layer_y: Vec::new(),
            layer_bounds: Vec::new(),
            deepest: Vec::new(),
            size: Vec2::ZERO,
            polyline_points: Vec::new(),
            polylines: Vec::new(),
            polyline_bounds: Vec::new(),
            polyline_reach: Vec::new(),
            reach_prefix: Vec::new(),
            rank_built: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
enum Vertex<K> {
    Real(K),
    Segment(K),
}

#[derive(Debug, Clone)]
struct Polyline<K> {
    from: K,
    to: K,
    start: usize,
    end: usize,
    min: Vec2,
    max: Vec2,
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    fn clear(&mut self, reserved_nodes: usize) {
        self.vertices.clear();
        self.vertices.reserve(reserved_nodes);
        self.top.clear();
        self.top.reserve(reserved_nodes);
        self.bottom.clear();
        self.bottom.reserve(reserved_nodes);

        self.real_offsets.clear();
        self.real_flat.clear();
        self.seg_top_offsets.clear();
        self.seg_top_flat.clear();
        self.seg_bottom_offsets.clear();
        self.seg_bottom_flat.clear();
        self.merged_top_offsets.clear();
        self.merged_top_flat.clear();
        self.merged_bottom_offsets.clear();
        self.merged_bottom_flat.clear();

        self.up_offsets.clear();
        self.up_flat.clear();
        self.down_offsets.clear();
        self.down_flat.clear();
        self.edge_list.clear();

        self.height = 0;

        self.indices.clear();
        self.sizes.clear();
        self.sizes.reserve(reserved_nodes);
        self.rank_half_width.clear();

        self.coordinates.clear();
        self.layer_y.clear();
        self.layer_bounds.clear();
        self.deepest.clear();
        self.size = Vec2::ZERO;
        self.polyline_points.clear();
        for bucket in &mut self.polylines {
            bucket.clear();
        }
        self.polyline_bounds.clear();
        self.polyline_reach.clear();
        self.reach_prefix.clear();
        self.rank_built.clear();
    }
    fn push_item(&mut self, vertex: Vertex<K>, top: usize, bottom: usize, size: Vec2) -> usize {
        let index = self.vertices.len();

        self.vertices.push(vertex);
        self.top.push(top);
        self.bottom.push(bottom);
        self.sizes.push(size);

        index
    }
    fn link(&mut self, from: usize, to: usize) {
        self.edge_list.push((from, to));
    }
    fn prepare_structure(&mut self) {
        #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
        {
            self.height = self.bottom.iter().copied().max().map_or(0, |rank| rank + 1);
        }

        if self.height > self.polylines.len() {
            self.polylines.resize_with(self.height, Vec::new);
        }

        self.rank_built.resize(self.height, false);
        self.polyline_bounds
            .resize(self.height, (Vec2::INFINITY, Vec2::NEG_INFINITY));
        self.polyline_reach.resize(self.height, (0.0_f32, 0.0_f32));

        let count = self.vertices.len();
        let ranks = self.height.strict_add(1);

        self.real_offsets.resize(ranks, 0);
        self.seg_top_offsets.resize(ranks, 0);
        self.seg_bottom_offsets.resize(ranks, 0);
        self.merged_top_offsets.resize(ranks, 0);
        self.merged_bottom_offsets.resize(ranks, 0);

        for ((vertex, top), bottom) in self
            .vertices
            .iter()
            .zip(self.top.iter().copied())
            .zip(self.bottom.iter().copied())
        {
            match vertex {
                Vertex::Real(_) => {
                    self.real_offsets[top] = self.real_offsets[top].strict_add(1);
                }
                Vertex::Segment(_) => {
                    self.seg_top_offsets[top] = self.seg_top_offsets[top].strict_add(1);
                    self.seg_bottom_offsets[bottom] = self.seg_bottom_offsets[bottom].strict_add(1);
                }
            }

            self.merged_top_offsets[top] = self.merged_top_offsets[top].strict_add(1);
            self.merged_bottom_offsets[bottom] = self.merged_bottom_offsets[bottom].strict_add(1);
        }

        let real_total = exclusive_prefix_sum(&mut self.real_offsets);
        let segment_total = exclusive_prefix_sum(&mut self.seg_top_offsets);

        exclusive_prefix_sum(&mut self.seg_bottom_offsets);
        exclusive_prefix_sum(&mut self.merged_top_offsets);
        exclusive_prefix_sum(&mut self.merged_bottom_offsets);

        self.real_flat.resize(real_total, 0);
        self.seg_top_flat.resize(segment_total, 0);
        self.seg_bottom_flat.resize(segment_total, 0);
        self.merged_top_flat.resize(count, 0);
        self.merged_bottom_flat.resize(count, 0);

        for (index, ((vertex, top), bottom)) in self
            .vertices
            .iter()
            .zip(self.top.iter().copied())
            .zip(self.bottom.iter().copied())
            .enumerate()
        {
            match vertex {
                Vertex::Real(_) => {
                    let cursor = self.real_offsets[top];

                    self.real_flat[cursor] = index;
                    self.real_offsets[top] = cursor.strict_add(1);
                }
                Vertex::Segment(_) => {
                    let cursor = self.seg_top_offsets[top];

                    self.seg_top_flat[cursor] = index;
                    self.seg_top_offsets[top] = cursor.strict_add(1);

                    let cursor = self.seg_bottom_offsets[bottom];

                    self.seg_bottom_flat[cursor] = index;
                    self.seg_bottom_offsets[bottom] = cursor.strict_add(1);
                }
            }

            let cursor = self.merged_top_offsets[top];

            self.merged_top_flat[cursor] = index;
            self.merged_top_offsets[top] = cursor.strict_add(1);

            let cursor = self.merged_bottom_offsets[bottom];

            self.merged_bottom_flat[cursor] = index;
            self.merged_bottom_offsets[bottom] = cursor.strict_add(1);
        }

        self.real_offsets.copy_within(0..self.height, 1);
        self.real_offsets[0] = 0;
        self.seg_top_offsets.copy_within(0..self.height, 1);
        self.seg_top_offsets[0] = 0;
        self.seg_bottom_offsets.copy_within(0..self.height, 1);
        self.seg_bottom_offsets[0] = 0;
        self.merged_top_offsets.copy_within(0..self.height, 1);
        self.merged_top_offsets[0] = 0;
        self.merged_bottom_offsets.copy_within(0..self.height, 1);
        self.merged_bottom_offsets[0] = 0;

        let edges = self.edge_list.len();

        self.down_offsets.resize(count.strict_add(1), 0);
        self.up_offsets.resize(count.strict_add(1), 0);

        for (source, target) in self.edge_list.iter().copied() {
            self.down_offsets[source] = self.down_offsets[source].strict_add(1);
            self.up_offsets[target] = self.up_offsets[target].strict_add(1);
        }

        let mut down_total = 0_usize;
        let mut up_total = 0_usize;

        for (down_offset, up_offset) in self.down_offsets.iter_mut().zip(&mut self.up_offsets) {
            let down_len = *down_offset;
            let up_len = *up_offset;

            *down_offset = down_total;
            *up_offset = up_total;
            down_total = down_total.strict_add(down_len);
            up_total = up_total.strict_add(up_len);
        }

        self.down_flat.resize(edges, (0, 0));

        for (edge, (source, target)) in self.edge_list.iter().copied().enumerate() {
            let cursor = self.down_offsets[source];

            self.down_flat[cursor] = (target, edge);
            self.down_offsets[source] = cursor.strict_add(1);
        }

        self.down_offsets.copy_within(0..count, 1);
        self.down_offsets[0] = 0;

        self.up_flat.resize(edges, (0, 0));

        for source in self.merged_bottom_flat.iter().copied() {
            for (target, edge) in self.down_flat
                [self.down_offsets[source]..self.down_offsets[source.strict_add(1)]]
                .iter()
                .copied()
            {
                let cursor = self.up_offsets[target];

                self.up_flat[cursor] = (source, edge);
                self.up_offsets[target] = cursor.strict_add(1);
            }
        }

        self.up_offsets.copy_within(0..count, 1);
        self.up_offsets[0] = 0;

        self.deepest.extend(0..self.height);

        for ((start, end), deepest) in spans(&self.real_offsets).zip(&mut self.deepest) {
            for source in self.real_flat[start..end].iter().copied() {
                for (target, _) in self.down_flat
                    [self.down_offsets[source]..self.down_offsets[source.strict_add(1)]]
                    .iter()
                    .copied()
                {
                    let child = match self.vertices[target] {
                        Vertex::Real(_) => self.top[target],
                        Vertex::Segment(_) => self.bottom[target].strict_add(1),
                    };

                    *deepest = (*deepest).max(child);
                }
            }
        }
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
        mut sizes: F,
        spacing: &Spacing,
    ) where
        T: IndependentContents,
        F: FnMut(&K) -> Vec2,
    {
        self.clear(weave.nodes.len());

        let mut processed = 0_usize;

        {
            let count = weave.nodes.len();
            let guard = weave.scratchpad.guard();

            let mut identifier_map = guard.map_with_capacity(count, S::default());
            let mut keys: ScratchpadVec<'_, K> = guard.vec_with_capacity(count);
            let mut remaining: ScratchpadVec<'_, usize> = guard.vec_with_capacity(count);
            let mut parent_offsets: ScratchpadVec<'_, usize> =
                guard.vec_with_capacity(count.strict_add(1));
            let mut child_offsets: ScratchpadVec<'_, usize> =
                guard.vec_with_capacity(count.strict_add(1));

            let mut parent_total = 0_usize;
            let mut child_total = 0_usize;

            parent_offsets.push(0);
            child_offsets.push(0);

            for (dense, (&id, node)) in weave.nodes.iter().enumerate() {
                identifier_map.insert(id, dense);
                keys.push(id);
                remaining.push(node.from.len());

                parent_total = parent_total.strict_add(node.from.len());
                child_total = child_total.strict_add(node.to.len());

                parent_offsets.push(parent_total);
                child_offsets.push(child_total);
            }

            let mut parent_flat: ScratchpadVec<'_, usize> = guard.vec_with_capacity(parent_total);
            let mut child_flat: ScratchpadVec<'_, usize> = guard.vec_with_capacity(child_total);

            for node in weave.nodes.values() {
                parent_flat.extend(node.from.iter().map(|id| identifier_map[id]));
                child_flat.extend(node.to.iter().map(|id| identifier_map[id]));
            }

            let mut vertex_of: ScratchpadVec<'_, usize> = guard.vec_with_capacity(count);
            let mut stack: ScratchpadVec<'_, usize> = guard.vec();
            let mut parents: ScratchpadVec<'_, (usize, usize)> = guard.vec_with_capacity(count);

            vertex_of.resize(count, usize::MAX);

            stack.extend(weave.roots.iter().rev().map(|id| identifier_map[id]));

            while let Some(dense) = stack.pop() {
                parents.extend(
                    parent_flat[parent_offsets[dense]..parent_offsets[dense.strict_add(1)]]
                        .iter()
                        .map(|&parent| {
                            let index = vertex_of[parent];
                            (index, self.top[index])
                        }),
                );

                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                let rank = parents
                    .iter()
                    .map(|&(_, rank)| rank)
                    .max()
                    .map_or(0, |rank| rank + 1);

                let id = keys[dense];
                let size = sizes(&id);

                assert!(validate_vec2(size), "Invalid size");

                let index = self.push_item(Vertex::Real(id), rank, rank, size);

                vertex_of[dense] = index;
                processed = processed.strict_add(1);

                for (from_index, from_rank) in parents.drain(..) {
                    #[allow(
                        clippy::arithmetic_side_effects,
                        reason = "Can never overflow or underflow"
                    )]
                    if from_rank + 1 == rank {
                        self.link(from_index, index);
                    } else {
                        let segment = self.push_item(
                            Vertex::Segment(id),
                            from_rank + 1,
                            rank - 1,
                            Vec2::ZERO,
                        );

                        self.link(from_index, segment);
                        self.link(segment, index);
                    }
                }

                for child in child_flat[child_offsets[dense]..child_offsets[dense.strict_add(1)]]
                    .iter()
                    .rev()
                    .copied()
                {
                    #[allow(clippy::arithmetic_side_effects, reason = "Can never underflow")]
                    {
                        remaining[child] -= 1;
                    }

                    if remaining[child] == 0 {
                        stack.push(child);
                    }
                }
            }
        }

        debug_assert_eq!(weave.nodes.len(), processed, "Malformed weave");

        self.prepare_structure();
        self.assign_dag_coordinates(&mut weave.scratchpad, spacing);
    }
    pub fn layout_topological<W, N, T, F>(
        &mut self,
        weave: &W,
        mut sizes: F,
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
        self.indices.reserve(weave.len());

        {
            let guard = scratchpad.guard();

            let mut parents: ScratchpadVec<'_, (usize, usize)> =
                guard.vec_with_capacity(weave.len());

            for id in topological.drain(..) {
                parents.extend(weave.get_parents(&id).unwrap().into_iter().map(|&id| {
                    let index = self.indices[&id];
                    (index, self.top[index])
                }));

                #[allow(clippy::arithmetic_side_effects, reason = "Can never overflow")]
                let rank = parents
                    .iter()
                    .map(|&(_, rank)| rank)
                    .max()
                    .map_or(0, |rank| rank + 1);

                let size = sizes(&id);

                assert!(validate_vec2(size), "Invalid size");

                let index = self.push_item(Vertex::Real(id), rank, rank, size);

                self.indices.insert(id, index);

                for (from_index, from_rank) in parents.drain(..) {
                    #[allow(
                        clippy::arithmetic_side_effects,
                        reason = "Can never overflow or underflow"
                    )]
                    if from_rank + 1 == rank {
                        self.link(from_index, index);
                    } else {
                        let segment = self.push_item(
                            Vertex::Segment(id),
                            from_rank + 1,
                            rank - 1,
                            Vec2::ZERO,
                        );

                        self.link(from_index, segment);
                        self.link(segment, index);
                    }
                }
            }
        }

        assert_eq!(
            weave.len(),
            self.indices.len(),
            "Malformed topological order"
        );

        self.prepare_structure();
        self.assign_dag_coordinates(scratchpad, spacing);
    }
}

struct PassScratch<'a, 'g> {
    marked: &'a [bool],
    extent: &'a [f32],
    margin: &'a [f32],
    segment: &'a [bool],
    leftmost_at: &'a [usize],
    rightmost_at: &'a [usize],
    left_offsets: &'a [usize],
    left_runs: &'a [(usize, usize, usize)],
    right_offsets: &'a [usize],
    right_runs: &'a [(usize, usize, usize)],
    root: &'a mut [usize],
    align: &'a mut [usize],
    sink: &'a mut [usize],
    shift: &'a mut [f32],
    stack: &'a mut ScratchpadVec<'g, (usize, usize, usize, bool)>,
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn assign_dag_coordinates(&mut self, scratchpad: &mut Scratchpad, spacing: &Spacing) {
        const PASSES: [(bool, bool); 4] =
            [(true, true), (true, false), (false, true), (false, false)];

        assert!(spacing.validate(), "Invalid spacing");

        let count = self.vertices.len();

        if count == 0 {
            return;
        }

        self.coordinates.resize(count, Vec2::ZERO);
        self.rank_half_width.clear();
        self.rank_half_width.resize(self.height, 0.0_f32);

        let guard = scratchpad.guard();

        let mut extent = guard.vec_with_capacity(count);
        let mut margin = guard.vec_with_capacity(count);
        let mut segment = guard.vec_with_capacity(count);
        let mut rank_tallest = guard.vec_with_capacity(self.height);
        let mut candidates = [
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
            guard.vec_with_capacity(count),
        ];

        extent.resize(count, 0.0_f32);
        margin.resize(count, 0.0_f32);
        segment.resize(count, false);
        rank_tallest.resize(self.height, 0.0_f32);
        for candidate in &mut candidates {
            candidate.resize(count, 0.0_f32);
        }

        for (((((vertex, size), rank), extent), margin), segment) in self
            .vertices
            .iter()
            .zip(self.sizes.iter().copied())
            .zip(self.top.iter().copied())
            .zip(extent.iter_mut())
            .zip(margin.iter_mut())
            .zip(segment.iter_mut())
        {
            match vertex {
                Vertex::Real(_) => {
                    let half_width = size.x * 0.5_f32;

                    *extent = half_width;
                    *margin = spacing.node;
                    *segment = false;

                    rank_tallest[rank] = rank_tallest[rank].max(size.y);
                    self.rank_half_width[rank] = self.rank_half_width[rank].max(half_width);
                }
                Vertex::Segment(_) => {
                    *extent = spacing.corridor * 0.5_f32;
                    *margin = spacing.edge;
                    *segment = true;
                }
            }
        }

        let mut marked = guard.vec_with_capacity(self.edge_list.len());
        let mut open_run_start = guard.vec_with_capacity(count);
        let mut leftmost_at = guard.vec_with_capacity(self.height);
        let mut rightmost_at = guard.vec_with_capacity(self.height);

        marked.resize(self.edge_list.len(), false);
        open_run_start.resize(count, 0_usize);
        leftmost_at.resize(self.height, 0_usize);
        rightmost_at.resize(self.height, 0_usize);

        let mut active = SlotSet::new(&guard);
        let mut spanning = SlotSet::new(&guard);

        active.rebuild(count);
        spanning.rebuild(count);

        let mut closed_runs = guard.vec_with_capacity(count.strict_mul(3));

        for rank in 0..=self.height {
            if let Some(previous) = rank.checked_sub(1) {
                for item in self.merged_bottom_flat[self.merged_bottom_offsets[previous]
                    ..self.merged_bottom_offsets[previous.strict_add(1)]]
                    .iter()
                    .copied()
                {
                    let before = active.predecessor(item);
                    let after = active.successor(item);

                    active.remove(item);

                    let end = rank.strict_sub(1);

                    if let Some(left) = before {
                        let start = open_run_start[left];
                        let right = item;

                        if end >= start {
                            closed_runs.push((left, right, start, end));
                        }
                    }
                    if let Some(right) = after {
                        let left = item;
                        let start = open_run_start[item];

                        if end >= start {
                            closed_runs.push((left, right, start, end));
                        }
                    }
                    if let (Some(left), Some(_)) = (before, after) {
                        open_run_start[left] = rank;
                    }
                }
            }

            if rank >= self.height {
                continue;
            }

            for item in self.merged_top_flat
                [self.merged_top_offsets[rank]..self.merged_top_offsets[rank.strict_add(1)]]
                .iter()
                .copied()
            {
                let after = active.successor(item);
                let before = active.predecessor(item);

                if let (Some(left), Some(right), Some(end)) = (before, after, rank.checked_sub(1)) {
                    let start = open_run_start[left];

                    if end >= start {
                        closed_runs.push((left, right, start, end));
                    }
                }

                if let Some(left) = before {
                    open_run_start[left] = rank;
                }
                if after.is_some() {
                    open_run_start[item] = rank;
                }

                active.insert(item);
            }

            leftmost_at[rank] = active.first().unwrap();
            rightmost_at[rank] = active.last().unwrap();

            if rank.strict_add(1) >= self.height {
                continue;
            }

            for segment in self.seg_top_flat
                [self.seg_top_offsets[rank]..self.seg_top_offsets[rank.strict_add(1)]]
                .iter()
                .copied()
            {
                spanning.insert(segment);
            }
            for segment in self.seg_bottom_flat
                [self.seg_bottom_offsets[rank]..self.seg_bottom_offsets[rank.strict_add(1)]]
                .iter()
                .copied()
            {
                spanning.remove(segment);
            }

            if spanning.is_empty() {
                continue;
            }

            for source in self.merged_bottom_flat
                [self.merged_bottom_offsets[rank]..self.merged_bottom_offsets[rank.strict_add(1)]]
                .iter()
                .copied()
            {
                let mut before: Option<Option<usize>> = None;
                let mut after: Option<Option<usize>> = None;

                for (target, edge) in self.down_flat
                    [self.down_offsets[source]..self.down_offsets[source.strict_add(1)]]
                    .iter()
                    .copied()
                {
                    let crossed = if target > source {
                        after
                            .get_or_insert_with(|| spanning.successor(source))
                            .is_some_and(|found| found < target)
                    } else {
                        before
                            .get_or_insert_with(|| spanning.predecessor(source))
                            .is_some_and(|found| found > target)
                    };

                    if crossed {
                        marked[edge] = true;
                    }
                }
            }
        }

        let total_runs = closed_runs.len();

        let mut left_offsets = guard.vec_with_capacity(count.strict_add(1));
        let mut right_offsets = guard.vec_with_capacity(count.strict_add(1));

        left_offsets.resize(count.strict_add(1), 0_usize);
        right_offsets.resize(count.strict_add(1), 0_usize);

        for (left, right, _, _) in closed_runs.iter().copied() {
            let after_left = left.strict_add(1);
            let after_right = right.strict_add(1);

            right_offsets[after_left] = right_offsets[after_left].strict_add(1);
            left_offsets[after_right] = left_offsets[after_right].strict_add(1);
        }

        let mut left_total = 0_usize;
        let mut right_total = 0_usize;

        for (left_offset, right_offset) in left_offsets.iter_mut().zip(right_offsets.iter_mut()) {
            left_total = left_total.strict_add(*left_offset);
            right_total = right_total.strict_add(*right_offset);

            *left_offset = left_total;
            *right_offset = right_total;
        }

        let mut left_runs = guard.vec_with_capacity(total_runs);
        let mut right_runs = guard.vec_with_capacity(total_runs);

        left_runs.resize(total_runs, (0_usize, 0_usize, 0_usize));
        right_runs.resize(total_runs, (0_usize, 0_usize, 0_usize));

        let mut left_cursors = guard.vec_with_capacity(count);
        let mut right_cursors = guard.vec_with_capacity(count);

        left_cursors.extend_from_slice(&left_offsets[..count]);
        right_cursors.extend_from_slice(&right_offsets[..count]);

        for (left, right, start, end) in closed_runs.iter().copied() {
            right_runs[right_cursors[left]] = (right, start, end);
            right_cursors[left] = right_cursors[left].strict_add(1);

            left_runs[left_cursors[right]] = (left, start, end);
            left_cursors[right] = left_cursors[right].strict_add(1);
        }

        let mut root = guard.vec_with_capacity(count);
        let mut align = guard.vec_with_capacity(count);
        let mut sink = guard.vec_with_capacity(count);
        let mut shift = guard.vec_with_capacity(count);

        root.resize(count, 0_usize);
        align.resize(count, 0_usize);
        sink.resize(count, 0_usize);
        shift.resize(count, 0.0_f32);

        let mut stack = guard.vec();

        let mut scratch = PassScratch {
            marked: &marked,
            extent: &extent,
            margin: &margin,
            segment: &segment,
            leftmost_at: &leftmost_at,
            rightmost_at: &rightmost_at,
            left_offsets: &left_offsets,
            left_runs: &left_runs,
            right_offsets: &right_offsets,
            right_runs: &right_runs,
            root: &mut root,
            align: &mut align,
            sink: &mut sink,
            shift: &mut shift,
            stack: &mut stack,
        };

        let [first, second, third, fourth] = &mut candidates;

        let extents = [
            self.coordinate_pass::<true, true>(&mut scratch, spacing, first),
            self.coordinate_pass::<true, false>(&mut scratch, spacing, second),
            self.coordinate_pass::<false, true>(&mut scratch, spacing, third),
            self.coordinate_pass::<false, false>(&mut scratch, spacing, fourth),
        ];

        let mut best = 0_usize;

        for pass in 1..4_usize {
            if extents[pass].1 - extents[pass].0 < extents[best].1 - extents[best].0 {
                best = pass;
            }
        }

        let mut offsets = [0.0_f32; 4];

        for (pass, (_, leftward)) in PASSES.into_iter().enumerate() {
            offsets[pass] = if leftward {
                extents[best].0 - extents[pass].0
            } else {
                extents[best].1 - extents[pass].1
            };
        }

        let mut left = f32::INFINITY;
        let mut right = f32::NEG_INFINITY;

        let [first, second, third, fourth] = &candidates;

        for (((((coordinate, a), b), c), d), extent) in self
            .coordinates
            .iter_mut()
            .zip(first.iter().copied())
            .zip(second.iter().copied())
            .zip(third.iter().copied())
            .zip(fourth.iter().copied())
            .zip(extent.iter().copied())
        {
            let (a, b, c, d) = (
                a + offsets[0],
                b + offsets[1],
                c + offsets[2],
                d + offsets[3],
            );

            let low = a.min(b).max(c.min(d));
            let high = a.max(b).min(c.max(d));
            let combined = (low + high) * 0.5_f32;

            coordinate.x = combined;
            left = left.min(combined - extent);
            right = right.max(combined + extent);
        }

        let mut valid = true;

        for coordinate in &mut self.coordinates {
            coordinate.x -= left;

            valid &= validate_float(coordinate.x);
        }

        self.layer_y.clear();
        self.layer_y.resize(self.height, 0.0_f32);
        self.layer_bounds.clear();
        self.layer_bounds.resize(self.height, (0.0_f32, 0.0_f32));

        let mut cursor = 0.0_f32;

        for (rank, (((&tallest, layer_y), layer_bounds), (top_start, top_end))) in rank_tallest
            .iter()
            .zip(&mut self.layer_y)
            .zip(&mut self.layer_bounds)
            .zip(spans(&self.merged_top_offsets))
            .enumerate()
        {
            let start = if rank == 0 {
                0.0_f32
            } else {
                cursor + spacing.layer
            };
            let end = start + tallest;
            let y = start + tallest * 0.5_f32;

            *layer_y = y;
            *layer_bounds = (start, end);

            valid &= validate_float(start) && validate_float(end) && validate_float(y);

            for vertex in self.merged_top_flat[top_start..top_end].iter().copied() {
                self.coordinates[vertex].y = y;
            }

            cursor = end;
        }

        self.reach_prefix.clear();
        self.reach_prefix.reserve(self.height);

        let mut reach = 0.0_f32;

        for deep in self.deepest.iter().copied() {
            reach = reach.max(self.layer_bounds[deep].1);
            self.reach_prefix.push(reach);
        }

        self.size = Vec2::new(right - left, cursor);

        valid &= validate_vec2(self.size);

        assert!(valid, "Output is not normal and positive");
    }
    #[allow(
        clippy::float_arithmetic,
        clippy::integer_division,
        clippy::integer_division_remainder_used,
        reason = "Coordinate calculation"
    )]
    fn coordinate_pass<const DOWNWARD: bool, const LEFTWARD: bool>(
        &self,
        scratch: &mut PassScratch<'_, '_>,
        spacing: &Spacing,
        x: &mut [f32],
    ) -> (f32, f32) {
        let PassScratch {
            marked,
            extent,
            margin,
            segment,
            leftmost_at,
            rightmost_at,
            left_offsets,
            left_runs,
            right_offsets,
            right_runs,
            root,
            align,
            sink,
            shift,
            stack,
        } = scratch;

        let height = self.height;

        let edge_le_node = spacing.edge <= spacing.node;
        let separation = |a: usize, b: usize| {
            extent[a]
                + extent[b]
                + if edge_le_node {
                    margin[a].min(margin[b])
                } else {
                    margin[a].max(margin[b])
                }
        };

        for (vertex, ((root, align), sink)) in root
            .iter_mut()
            .zip(align.iter_mut())
            .zip(sink.iter_mut())
            .enumerate()
        {
            *root = vertex;
            *align = vertex;
            *sink = vertex;
        }

        shift.fill(f32::INFINITY);
        x.fill(f32::NAN);

        for step in 0..height {
            let rank = reflect(step, height, DOWNWARD);

            let layer = if DOWNWARD {
                &self.merged_top_flat
                    [self.merged_top_offsets[rank]..self.merged_top_offsets[rank.strict_add(1)]]
            } else {
                &self.merged_bottom_flat[self.merged_bottom_offsets[rank]
                    ..self.merged_bottom_offsets[rank.strict_add(1)]]
            };

            let mut last: Option<usize> = None;

            for vertex in directed(layer, LEFTWARD).copied() {
                let neighbours: &[(usize, usize)] = if DOWNWARD {
                    &self.up_flat[self.up_offsets[vertex]..self.up_offsets[vertex.strict_add(1)]]
                } else {
                    &self.down_flat
                        [self.down_offsets[vertex]..self.down_offsets[vertex.strict_add(1)]]
                };
                let degree = neighbours.len();

                if degree == 0 {
                    continue;
                }

                let mut medians = [
                    reflect(degree.strict_sub(1) / 2, degree, LEFTWARD),
                    reflect(degree / 2, degree, LEFTWARD),
                ];

                let is_segment = |median: usize| segment[neighbours[median].0];

                let distinct = if medians[0] == medians[1] {
                    1
                } else {
                    if !is_segment(medians[0]) && is_segment(medians[1]) {
                        medians.swap(0, 1);
                    }

                    2
                };

                for median in medians[..distinct].iter().copied() {
                    if align[vertex] != vertex {
                        continue;
                    }

                    let (neighbour, edge) = neighbours[median];

                    let admissible = last.is_none_or(|last| {
                        if LEFTWARD {
                            last < neighbour
                        } else {
                            last > neighbour
                        }
                    });

                    if !marked[edge] && admissible {
                        align[neighbour] = vertex;
                        root[vertex] = root[neighbour];
                        align[vertex] = root[vertex];

                        last = Some(neighbour);
                    }
                }
            }
        }

        stack.clear();

        for rank in 0..height {
            let layer = &self.merged_top_flat
                [self.merged_top_offsets[rank]..self.merged_top_offsets[rank.strict_add(1)]];

            for start in directed(layer, LEFTWARD).copied() {
                if root[start] != start || !x[start].is_nan() {
                    continue;
                }

                stack.push((start, start, 0, false));

                while let Some((root_val, member, runs_applied, started)) = stack.last().copied() {
                    if !started {
                        x[root_val] = 0.0_f32;
                        stack.last_mut().unwrap().3 = true;
                    }

                    let runs = if LEFTWARD {
                        &left_runs[left_offsets[member]..left_offsets[member.strict_add(1)]]
                    } else {
                        &right_runs[right_offsets[member]..right_offsets[member.strict_add(1)]]
                    };

                    let mut applied = runs_applied;
                    let mut nested = false;

                    while applied < runs.len() {
                        let run = reflect(applied, runs.len(), DOWNWARD);
                        let (neighbour, _, _) = runs[run];
                        let neighbour_root = root[neighbour];

                        if x[neighbour_root].is_nan() {
                            stack.last_mut().unwrap().2 = applied;
                            stack.push((neighbour_root, neighbour_root, 0, false));

                            nested = true;
                            break;
                        }

                        if sink[root_val] == root_val {
                            sink[root_val] = sink[neighbour_root];
                        }

                        if sink[root_val] == sink[neighbour_root] {
                            x[root_val] =
                                x[root_val].max(x[neighbour_root] + separation(neighbour, member));
                        }

                        applied = applied.strict_add(1);
                    }

                    if nested {
                        continue;
                    }

                    let next = align[member];

                    if next == root_val {
                        let mut member = root_val;

                        while align[member] != root_val {
                            member = align[member];

                            x[member] = x[root_val];
                            sink[member] = sink[root_val];
                        }

                        stack.pop();
                    } else {
                        let frame = stack.last_mut().unwrap();

                        frame.1 = next;
                        frame.2 = 0;
                    }
                }
            }
        }

        for step in 0..height {
            let rank = reflect(step, height, DOWNWARD);

            let entry = if LEFTWARD {
                leftmost_at[rank]
            } else {
                rightmost_at[rank]
            };

            if sink[entry] != entry {
                continue;
            }

            let entry_rank = if DOWNWARD {
                self.top[entry]
            } else {
                self.bottom[entry]
            };

            if rank != entry_rank {
                continue;
            }

            if !shift[entry].is_finite() {
                shift[entry] = 0.0_f32;
            }

            let mut vertex = entry;
            let mut from = rank;

            loop {
                let runs = if LEFTWARD {
                    &left_runs[left_offsets[vertex]..left_offsets[vertex.strict_add(1)]]
                } else {
                    &right_runs[right_offsets[vertex]..right_offsets[vertex.strict_add(1)]]
                };

                for (neighbour, start, end) in runs.iter().copied() {
                    let forward = if DOWNWARD { end > from } else { start < from };

                    if !forward {
                        continue;
                    }

                    let neighbour_sink = sink[neighbour];

                    shift[neighbour_sink] = shift[neighbour_sink].min(
                        shift[sink[vertex]] + x[vertex]
                            - (x[neighbour] + separation(neighbour, vertex)),
                    );
                }

                while align[vertex] != root[vertex] {
                    vertex = align[vertex];

                    let runs = if LEFTWARD {
                        &left_runs[left_offsets[vertex]..left_offsets[vertex.strict_add(1)]]
                    } else {
                        &right_runs[right_offsets[vertex]..right_offsets[vertex.strict_add(1)]]
                    };

                    for (neighbour, _, _) in runs.iter().copied() {
                        let neighbour_sink = sink[neighbour];

                        shift[neighbour_sink] = shift[neighbour_sink].min(
                            shift[sink[vertex]] + x[vertex]
                                - (x[neighbour] + separation(neighbour, vertex)),
                        );
                    }
                }

                let across = if DOWNWARD {
                    self.bottom[vertex]
                } else {
                    self.top[vertex]
                };

                let runs = if LEFTWARD {
                    &right_runs[right_offsets[vertex]..right_offsets[vertex.strict_add(1)]]
                } else {
                    &left_runs[left_offsets[vertex]..left_offsets[vertex.strict_add(1)]]
                };
                let next = if DOWNWARD {
                    runs.last()
                        .and_then(|&(neighbour, _, end)| (end == across).then_some(neighbour))
                } else {
                    runs.first()
                        .and_then(|&(neighbour, start, _)| (start == across).then_some(neighbour))
                };

                let Some(next) = next else {
                    break;
                };

                if sink[next] != sink[vertex] {
                    break;
                }

                vertex = next;
                from = across;
            }
        }

        let mut minimum = f32::INFINITY;
        let mut maximum = f32::NEG_INFINITY;

        for ((x, sink), extent) in x
            .iter_mut()
            .zip(sink.iter().copied())
            .zip(extent.iter().copied())
        {
            *x += shift[sink];

            if !LEFTWARD {
                *x = -*x;
            }

            minimum = minimum.min(*x - extent);
            maximum = maximum.max(*x + extent);
        }

        (minimum, maximum)
    }
}

impl<K, S> Layout2D<K, S>
where
    K: Hash + Copy + Eq + Ord,
    S: BuildHasher + Default + Clone,
{
    #[allow(clippy::float_arithmetic, reason = "Coordinate calculation")]
    fn build_polylines(&mut self, min: Vec2, max: Vec2) {
        let first = self.reach_prefix.partition_point(|&reach| reach < min.y);
        let last = self
            .layer_bounds
            .partition_point(|&(start, _)| start <= max.y);

        for rank in first..last {
            if self.rank_built[rank] || self.layer_bounds[self.deepest[rank]].1 < min.y {
                continue;
            }

            self.rank_built[rank] = true;

            let mut rank_min = Vec2::INFINITY;
            let mut rank_max = Vec2::NEG_INFINITY;
            let mut left_reach = 0.0_f32;
            let mut right_reach = 0.0_f32;

            for source in self.real_flat
                [self.real_offsets[rank]..self.real_offsets[rank.strict_add(1)]]
                .iter()
                .copied()
            {
                let Vertex::Real(from) = self.vertices[source] else {
                    continue;
                };

                let source_x = self.coordinates[source].x;
                let source_border = self.coordinates[source].y + self.sizes[source].y * 0.5_f32;
                let source_band_end = self.layer_bounds[rank].1;

                for (target, _) in self.down_flat
                    [self.down_offsets[source]..self.down_offsets[source.strict_add(1)]]
                    .iter()
                    .copied()
                {
                    let (to, real_target) = match self.vertices[target] {
                        Vertex::Real(to) => (to, target),
                        Vertex::Segment(to) => (to, self.down_flat[self.down_offsets[target]].0),
                    };

                    let target_x = self.coordinates[real_target].x;
                    let target_border =
                        self.coordinates[real_target].y - self.sizes[real_target].y * 0.5_f32;

                    let start = self.polyline_points.len();
                    let first = Vec2::new(source_x, source_border);
                    let mut line_min = first;
                    let mut line_max = first;

                    self.polyline_points.push(first);

                    if source_band_end > source_border {
                        let point = Vec2::new(source_x, source_band_end);

                        line_min = line_min.min(point);
                        line_max = line_max.max(point);
                        self.polyline_points.push(point);
                    }

                    if let Vertex::Segment(_) = self.vertices[target] {
                        let x = self.coordinates[target].x;

                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(x, self.layer_bounds[self.top[target]].0),
                            &mut line_min,
                            &mut line_max,
                        );
                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(x, self.layer_bounds[self.bottom[target]].1),
                            &mut line_min,
                            &mut line_max,
                        );
                    }

                    let target_band_start = self.layer_bounds[self.top[real_target]].0;

                    if target_band_start < target_border {
                        push_deduplicated(
                            &mut self.polyline_points,
                            Vec2::new(target_x, target_band_start),
                            &mut line_min,
                            &mut line_max,
                        );
                    }

                    push_deduplicated(
                        &mut self.polyline_points,
                        Vec2::new(target_x, target_border),
                        &mut line_min,
                        &mut line_max,
                    );

                    if self.polyline_points.len() == start.strict_add(1) {
                        self.polyline_points
                            .push(Vec2::new(target_x, target_border));
                    }

                    rank_min = rank_min.min(line_min);
                    rank_max = rank_max.max(line_max);
                    left_reach = left_reach.max(source_x - line_min.x);
                    right_reach = right_reach.max(line_max.x - source_x);

                    self.polylines[rank].push(Polyline {
                        from,
                        to,
                        start,
                        end: self.polyline_points.len(),
                        min: line_min,
                        max: line_max,
                    });
                }
            }

            self.polyline_bounds[rank] = (rank_min, rank_max);
            self.polyline_reach[rank] = (left_reach, right_reach);
        }
    }
    #[must_use]
    pub const fn size(&self) -> Vec2 {
        self.size
    }
    #[allow(
        clippy::float_arithmetic,
        clippy::arithmetic_side_effects,
        reason = "Coordinate calculation"
    )]
    pub fn view<'a, F>(&'a mut self, min: Vec2, max: Vec2, mut callback: F)
    where
        F: FnMut(LayoutItem<'a, K, Vec2>),
    {
        if !self.edge_list.is_empty() {
            self.build_polylines(min, max);
        }

        let first = self.layer_bounds.partition_point(|&(_, end)| end < min.y);
        let first_reaching = self.reach_prefix.partition_point(|&reach| reach < min.y);
        let last = self
            .layer_bounds
            .partition_point(|&(start, _)| start <= max.y);

        for ((lines, (rank_min, rank_max)), (left_reach, right_reach)) in self
            .polylines
            .iter()
            .zip(self.polyline_bounds.iter().copied())
            .zip(self.polyline_reach.iter().copied())
            .skip(first_reaching)
            .take(last.saturating_sub(first_reaching))
        {
            if !(rank_min.x <= max.x
                && rank_max.x >= min.x
                && rank_min.y <= max.y
                && rank_max.y >= min.y)
            {
                continue;
            }

            let begin = lines
                .partition_point(|line| self.polyline_points[line.start].x < min.x - right_reach);

            for line in &lines[begin..] {
                if self.polyline_points[line.start].x - left_reach > max.x {
                    break;
                }

                if line.min.x <= max.x
                    && line.max.x >= min.x
                    && line.min.y <= max.y
                    && line.max.y >= min.y
                {
                    callback(LayoutItem::Polyline {
                        from: line.from,
                        to: line.to,
                        points: &self.polyline_points[line.start..line.end],
                    });
                }
            }
        }

        for ((start, end), half_width) in spans(&self.real_offsets)
            .zip(self.rank_half_width.iter().copied())
            .skip(first)
            .take(last.saturating_sub(first))
        {
            let reals = &self.real_flat[start..end];

            let cutoff = min.x - half_width;
            let begin = reals.partition_point(|&vertex| self.coordinates[vertex].x < cutoff);

            for vertex in reals[begin..].iter().copied() {
                let Vertex::Real(id) = self.vertices[vertex] else {
                    continue;
                };

                let center = self.coordinates[vertex];
                let size = self.sizes[vertex];
                let half = size * 0.5_f32;

                if center.x - half.x > max.x {
                    break;
                }

                if center.x + half.x >= min.x
                    && center.y - half.y <= max.y
                    && center.y + half.y >= min.y
                {
                    callback(LayoutItem::Node { id, center, size });
                }
            }
        }
    }
}

#[inline]
fn directed<T>(slice: &[T], forward: bool) -> impl Iterator<Item = &T> {
    let mut items = slice.iter();

    iter::from_fn(move || {
        if forward {
            items.next()
        } else {
            items.next_back()
        }
    })
}

#[inline]
fn spans(offsets: &[usize]) -> impl Iterator<Item = (usize, usize)> {
    offsets
        .iter()
        .zip(offsets.iter().skip(1))
        .map(|(start, end)| (*start, *end))
}

#[inline]
const fn reflect(index: usize, len: usize, forward: bool) -> usize {
    if forward {
        index
    } else {
        len.strict_sub(1).strict_sub(index)
    }
}

fn push_deduplicated(points: &mut Vec<Vec2>, point: Vec2, min: &mut Vec2, max: &mut Vec2) {
    *min = min.min(point);
    *max = max.max(point);

    if points.last().is_none_or(|&last| last != point) {
        points.push(point);
    }
}

fn exclusive_prefix_sum(offsets: &mut [usize]) -> usize {
    let mut total = 0_usize;

    for offset in offsets {
        let len = *offset;

        *offset = total;
        total = total.strict_add(len);
    }

    total
}
