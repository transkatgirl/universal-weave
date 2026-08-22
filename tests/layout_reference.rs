use std::hash::Hash;

use glam::Vec2;
use hashbrown::HashMap;
use rust_sugiyama_fork::{
    PairSeparation,
    algorithm::{Edge, Vertex, p3_calculate_coordinates},
    petgraph::stable_graph::{NodeIndex, StableDiGraph},
};
use universal_weave::{LayoutItem, Layouter, Node, Weave, layout::Spacing};

struct ReferenceLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    spacing: Spacing,
    size: Vec2,
    nodes: Vec<(K, Vec2, Vec2)>,
    polylines: Vec<(K, K, Vec2, Vec2, Vec<Vec2>)>,
}

impl<K> ReferenceLayouter<K>
where
    K: Hash + Copy + Eq + Ord,
{
    fn new(spacing: Spacing) -> Self {
        Self {
            spacing,
            size: Vec2::ZERO,
            nodes: Vec::new(),
            polylines: Vec::new(),
        }
    }
}

struct GraphBuilder<K> {
    vertices: Vec<VertexInfo<K>>,
    layers: Vec<Vec<NodeIndex>>,
    graph: StableDiGraph<Vertex, Edge>,
}

struct VertexInfo<K> {
    id: Option<K>,
    rank: usize,
    next: Vec<usize>,
}

impl<K> GraphBuilder<K> {
    fn new() -> Self {
        Self {
            vertices: Vec::new(),
            layers: Vec::new(),
            graph: StableDiGraph::new(),
        }
    }
    fn push(&mut self, id: Option<K>, size: Vec2, rank: usize) -> usize {
        let mut node = Vertex::new(self.vertices.len(), (size.x as f64, size.y as f64));
        node.is_dummy = id.is_none();

        let index = self.graph.add_node(node);

        assert_eq!(node.id, index.index());

        if rank == self.layers.len() {
            self.layers.push(Vec::new());
        }

        self.layers[rank].push(index);
        self.vertices.push(VertexInfo {
            id,
            rank,
            next: Vec::new(),
        });

        index.index()
    }
    fn link(&mut self, from: usize, to: usize) {
        assert_eq!(self.vertices[from].rank + 1, self.vertices[to].rank);

        self.vertices[from].next.push(to);
        self.graph
            .add_edge(NodeIndex::new(from), NodeIndex::new(to), Edge::default());
    }
}

impl<W, K, N, T> Layouter<W, K, N, T, Vec2> for ReferenceLayouter<K>
where
    W: Weave<K, N, T>,
    K: Hash + Copy + Eq + Ord,
    N: Node<K, T>,
    for<'a> &'a N::From: IntoIterator<Item = &'a K>,
{
    fn layout(&mut self, weave: &mut W, mut sizes: impl FnMut(&K) -> Vec2) {
        assert!(self.spacing.validate());
        self.size = Vec2::ZERO;
        self.nodes.clear();
        self.polylines.clear();

        let mut order = Vec::with_capacity(weave.len());

        weave.get_ordered_identifiers(&mut order);

        assert_eq!(weave.len(), order.len());

        if order.is_empty() {
            return;
        }

        let mut builder = GraphBuilder::<K>::new();
        let mut indices: HashMap<K, usize> = HashMap::with_capacity(order.len());

        for id in order {
            let parents = weave.get_parents(&id).unwrap();

            let rank = parents
                .into_iter()
                .map(|id| builder.vertices[indices[id]].rank)
                .max()
                .map_or(0, |r| r + 1);

            let size = sizes(&id);

            assert!(
                matches!(
                    size.x.classify(),
                    core::num::FpCategory::Normal | core::num::FpCategory::Zero
                ) && size.x.is_sign_positive()
                    && matches!(
                        size.y.classify(),
                        core::num::FpCategory::Normal | core::num::FpCategory::Zero
                    )
                    && size.y.is_sign_positive()
            );

            let index = builder.push(Some(id), size, rank);

            assert!(indices.insert(id, index).is_none());

            for mut from in parents.into_iter().map(|id| indices[id]) {
                for dummy_rank in builder.vertices[from].rank + 1..rank {
                    let dummy =
                        builder.push(None, Vec2::new(self.spacing.corridor, 0.0), dummy_rank);

                    builder.link(from, dummy);
                    from = dummy;
                }

                builder.link(from, index);
            }
        }

        let mut layouts = p3_calculate_coordinates::create_layouts(
            &mut builder.graph,
            &mut builder.layers,
            Some(PairSeparation {
                vertex_gap: self.spacing.node as f64,
                edge_gap: self.spacing.edge as f64,
            }),
        );

        p3_calculate_coordinates::align_to_smallest_width_layout(&builder.graph, &mut layouts);

        let mut x = vec![0.0_f64; builder.vertices.len()];

        for (index, coordinate) in p3_calculate_coordinates::calculate_relative_coords(layouts) {
            x[index.index()] = coordinate;
        }

        let (left, right) = x.iter().copied().zip(builder.graph.node_weights()).fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(left, right), (coordinate, vertex)| {
                (
                    left.min(coordinate - vertex.size.0 * 0.5),
                    right.max(coordinate + vertex.size.0 * 0.5),
                )
            },
        );

        let x: Vec<f32> = x
            .into_iter()
            .map(|coordinate| (coordinate - left) as f32)
            .collect();

        let mut bands = Vec::with_capacity(builder.layers.len());

        for members in &builder.layers {
            let start = bands
                .last()
                .map_or(0.0, |&(_, end)| end + self.spacing.layer as f64);

            bands.push((
                start,
                start
                    + members
                        .iter()
                        .copied()
                        .map(|index| builder.graph[index].size.1)
                        .fold(0.0_f64, f64::max),
            ));
        }

        self.size = Vec2::new((right - left) as f32, bands.last().unwrap().1 as f32);

        let center = |rank: usize| (bands[rank].0 + bands[rank].1) * 0.5;

        for (rank, members) in builder.layers.into_iter().enumerate() {
            let rank_center = center(rank);
            let rank_band_end = bands[rank].1;

            for source in members.into_iter().map(|index| index.index()) {
                let Some(source_id) = builder.vertices[source].id else {
                    continue;
                };

                let source_x = x[source];
                let source_size = builder.graph[NodeIndex::new(source)].size;

                self.nodes.push((
                    source_id,
                    Vec2::new(source_x, rank_center as f32),
                    Vec2::new(source_size.0 as f32, source_size.1 as f32),
                ));

                for target in builder.vertices[source].next.iter().copied() {
                    let mut final_target = target;

                    let target_key = loop {
                        match builder.vertices[final_target].id {
                            Some(key) => break key,
                            None => final_target = builder.vertices[final_target].next[0],
                        }
                    };

                    let target_rank = builder.vertices[final_target].rank;
                    let target_x = x[final_target];
                    let target_size = builder.graph[NodeIndex::new(final_target)].size;

                    let mut points = vec![
                        Vec2::new(source_x, (rank_center + source_size.1 * 0.5) as f32),
                        Vec2::new(source_x, rank_band_end as f32),
                    ];

                    if final_target != target {
                        let segment_x = x[target];

                        points.push(Vec2::new(segment_x, bands[rank + 1].0 as f32));
                        points.push(Vec2::new(segment_x, bands[target_rank - 1].1 as f32));
                    }

                    points.push(Vec2::new(target_x, bands[target_rank].0 as f32));
                    points.push(Vec2::new(
                        target_x,
                        (center(target_rank) - target_size.1 * 0.5) as f32,
                    ));

                    points.dedup();
                    let (min, max) = points.iter().fold(
                        (Vec2::INFINITY, Vec2::NEG_INFINITY),
                        |(low, high), &point| (low.min(point), high.max(point)),
                    );
                    if points.len() == 1 {
                        points.push(points[0]);
                    }

                    self.polylines
                        .push((source_id, target_key, min, max, points));
                }
            }
        }
    }
    fn size(&self) -> Vec2 {
        self.size
    }
    fn view<'a>(
        &'a mut self,
        min: Vec2,
        max: Vec2,
        mut callback: impl FnMut(LayoutItem<'a, K, Vec2>),
    ) {
        for (from, to, line_min, line_max, points) in &self.polylines {
            if line_min.x <= max.x
                && line_max.x >= min.x
                && line_min.y <= max.y
                && line_max.y >= min.y
            {
                callback(LayoutItem::Polyline {
                    from: *from,
                    to: *to,
                    points,
                })
            }
        }

        for (id, center, size) in self.nodes.iter().copied() {
            let half = size * 0.5;

            if center.x + half.x >= min.x
                && center.x - half.x <= max.x
                && center.y - half.y <= max.y
                && center.y + half.y >= min.y
            {
                callback(LayoutItem::Node { id, center, size });
            }
        }
    }
}
