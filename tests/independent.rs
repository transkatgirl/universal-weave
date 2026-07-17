use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash, RandomState},
    ops::Index,
};

use indexmap::IndexSet;
use proptest::{collection::size_range, prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use stacksafe::stacksafe;
use universal_weave::{
    DiscreteContentResult, DiscreteContents, DiscreteWeave, IndependentContents,
    IndependentWeave as IndependentWeaveTrait, MetadataWeave, Node, SemiIndependentWeave,
    SortableWeave, Weave,
    independent::{IndependentNode, IndependentWeave},
};

const CASES: u32 = 16384;
const MAX_TRANSITIONS: usize = 512;

prop_state_machine! {
    #![proptest_config(Config {
        cases: CASES,
        failure_persistence: None,
        //verbose: 1,
        max_shrink_time: MAX_TRANSITIONS as u32 * 4000,
        max_shrink_iters: u32::MAX-1,
        //timeout: 1000,
        .. Config::default()
    })]

    #[test]
    fn run_state_machine(
        sequential
        1..MAX_TRANSITIONS
        =>
        WeaveWrapper
    );
}

struct WeaveStateMachine;

impl ReferenceStateMachine for WeaveStateMachine {
    type State = Vec<WeaveTransition>;
    type Transition = WeaveTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(Vec::with_capacity(MAX_TRANSITIONS)).boxed()
    }
    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        any::<Self::Transition>().boxed()
    }
    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        state.push(transition.clone());
        state
    }
}

#[derive(Arbitrary, Debug, Clone)]
enum WeaveTransition {
    GetOrderedNodeIdentifiers {
        reversed: bool,
    },
    GetOrderedNodeIdentifiersFrom {
        reversed: bool,
        id_seed: u32,
    },
    GetActiveThread,
    GetThreadFrom {
        id_seed: u32,
    },
    #[proptest(weight = 8)]
    AddNode {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 3)]
    AddNodeTo {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        to_seeds: Vec<u32>,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
        length: u32,
    },
    #[proptest(weight = 3)]
    SetNodeActiveStatus {
        alternate: bool,
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    SetNodeActiveStatusInPlace {
        value: bool,
        id_seed: u32,
    },
    SetNodeBookmarkedStatus {
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveNode {
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveNodeTracked {
        id_seed: u32,
    },
    RemoveAllNodes {
        apply_seed: u16,
    },
    MetadataMut {
        content_seed: u32,
    },
    SortNodeChildrenBy {
        id_seed: u32,
        sort_seed: u32,
    },
    SortNodeChildrenById {
        id_seed: u32,
        sort_seed: u32,
    },
    SortRootsBy {
        sort_seed: u32,
    },
    SortRootsById {
        sort_seed: u32,
    },
    SortBookmarksBy {
        sort_seed: u32,
    },
    SortBookmarksById {
        sort_seed: u32,
    },
    #[proptest(weight = 3)]
    MoveNode {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        new_parents_seeds: Vec<u32>,
        id_seed: u32,
    },
    GetContentsMut {
        id_seed: u32,
        content_seed: u32,
    },
    #[proptest(weight = 3)]
    SplitNode {
        at_seed: u32,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    MergeNodeWithParent {
        id_seed: u32,
    },
}

struct WeaveWrapper {
    weave: IndependentWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    scratchpad: Vec<u32>,
    scratchpad_set: HashSet<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WeaveContent {
    length: u32,
    content_seed: u32,
}

impl IndependentContents for WeaveContent {}

impl DiscreteContents for WeaveContent {
    fn split(self, at: usize) -> DiscreteContentResult<Self> {
        if at == 0 || at as u64 >= self.length as u64 {
            DiscreteContentResult::One(self)
        } else {
            let left = WeaveContent {
                length: at as u32,
                content_seed: self.content_seed,
            };
            let right = WeaveContent {
                length: self.length.saturating_sub(at as u32),
                content_seed: self.content_seed,
            };
            assert_eq!(left.length.saturating_add(right.length), self.length);
            assert_ne!(left.length, 0);
            assert_ne!(right.length, 0);

            DiscreteContentResult::Two(left, right)
        }
    }
    fn merge(self, value: Self) -> DiscreteContentResult<Self> {
        if self.content_seed == value.content_seed && !self.length.overflowing_add(value.length).1 {
            DiscreteContentResult::One(Self {
                length: self.length.saturating_add(value.length),
                content_seed: self.content_seed,
            })
        } else {
            DiscreteContentResult::Two(self, value)
        }
    }
}

// Invariants are validated by the function's contracts
impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        println!("init");
        WeaveWrapper {
            weave: IndependentWeave::with_capacity(ref_state.len(), ref_state.len() as u32),
            counter: 0,
            scratchpad: Vec::with_capacity(ref_state.len()),
            scratchpad_set: HashSet::with_capacity(ref_state.len()),
        }
    }
    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        let s = RandomState::default();
        let hash_value = |value: u64| s.hash_one(value);
        let map_id = |seed: u32| seed % (state.counter + 2);
        let old_node_count = state.weave.nodes().len();

        match transition {
            WeaveTransition::GetOrderedNodeIdentifiers { reversed } => {
                if reversed {
                    println!(
                        "weave.get_ordered_node_identifiers_reversed_children(&mut scratchpad);"
                    );
                    state
                        .weave
                        .get_ordered_node_identifiers_reversed_children(&mut state.scratchpad);
                } else {
                    println!("weave.get_ordered_node_identifiers(&mut scratchpad);");
                    state
                        .weave
                        .get_ordered_node_identifiers(&mut state.scratchpad);
                }
            }
            WeaveTransition::GetOrderedNodeIdentifiersFrom { id_seed, reversed } => {
                if reversed {
                    println!(
                        "weave.get_ordered_node_identifiers_from_reversed_children(&{}, &mut scratchpad);",
                        map_id(id_seed)
                    );
                    state
                        .weave
                        .get_ordered_node_identifiers_from_reversed_children(
                            &map_id(id_seed),
                            &mut state.scratchpad,
                        );
                } else {
                    println!(
                        "weave.get_ordered_node_identifiers_from(&{}, &mut scratchpad);",
                        map_id(id_seed)
                    );
                    state
                        .weave
                        .get_ordered_node_identifiers_from(&map_id(id_seed), &mut state.scratchpad);
                }
            }
            WeaveTransition::GetActiveThread => {
                println!("weave.get_active_thread(&mut scratchpad);");
                state.weave.get_active_thread(&mut state.scratchpad);
            }
            WeaveTransition::GetThreadFrom { id_seed } => {
                println!(
                    "weave.get_thread_from(&{}, &mut scratchpad);",
                    map_id(id_seed)
                );
                state
                    .weave
                    .get_thread_from(&map_id(id_seed), &mut state.scratchpad);
            }
            WeaveTransition::AddNode {
                from_seeds,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                let node = IndependentNode {
                    id: state.counter,
                    from: IndexSet::from_iter(from_seeds.into_iter().map(&map_id)),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: length % 64,
                        content_seed: content_seed % 4,
                    },
                };
                println!(
                    "weave.add_node(IndependentNode {{ id: {}, from: IndexSet::from_iter({:?}), to: IndexSet::default(), active: {}, bookmarked: {}, contents:  WeaveContent {{ length: {}, content_seed: {} }} }});",
                    node.id,
                    node.from.iter().copied().collect::<Vec<_>>(),
                    node.active,
                    node.bookmarked,
                    node.contents.length,
                    node.contents.content_seed
                );
                state.weave.add_node(node);
            }
            WeaveTransition::AddNodeTo {
                from_seeds,
                to_seeds,
                active,
                bookmarked,
                length,
                content_seed,
            } => {
                let from = IndexSet::from_iter(from_seeds.into_iter().map(&map_id));
                state.scratchpad_set.clear();
                for id in &from {
                    if state.weave.contains(id) {
                        downwards_subgraph(state.weave.nodes(), id, &mut state.scratchpad_set);
                    }
                }
                let mut to = IndexSet::with_capacity(to_seeds.len());
                for id in to_seeds.into_iter().map(&map_id) {
                    if !state.scratchpad_set.contains(&id) || !state.weave.contains(&id) {
                        if state.weave.contains(&id) {
                            downwards_subgraph(state.weave.nodes(), &id, &mut state.scratchpad_set);
                        }

                        to.insert(id);
                    }
                }

                let node = IndependentNode {
                    id: state.counter,
                    from,
                    to,
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: length % 64,
                        content_seed: content_seed % 4,
                    },
                };
                println!(
                    "weave.add_node(IndependentNode {{ id: {}, from: IndexSet::from_iter({:?}), to: IndexSet::from_iter({:?}), active: {}, bookmarked: {}, contents:  WeaveContent {{ length: {}, content_seed: {} }} }});",
                    node.id,
                    node.from.iter().copied().collect::<Vec<_>>(),
                    node.to.iter().copied().collect::<Vec<_>>(),
                    node.active,
                    node.bookmarked,
                    node.contents.length,
                    node.contents.content_seed
                );
                state.weave.add_node(node);
            }
            WeaveTransition::SetNodeActiveStatus {
                id_seed,
                value,
                alternate,
            } => {
                println!(
                    "weave.set_node_active_status(&{}, {}, {});",
                    map_id(id_seed),
                    value,
                    alternate
                );
                state
                    .weave
                    .set_node_active_status(&map_id(id_seed), value, alternate);
            }
            WeaveTransition::SetNodeActiveStatusInPlace { id_seed, value } => {
                println!(
                    "weave.set_node_active_status_in_place(&{}, {});",
                    map_id(id_seed),
                    value,
                );
                state
                    .weave
                    .set_node_active_status_in_place(&map_id(id_seed), value);
            }
            WeaveTransition::SetNodeBookmarkedStatus { id_seed, value } => {
                println!(
                    "weave.set_node_bookmarked_status(&{}, {});",
                    map_id(id_seed),
                    value
                );
                state
                    .weave
                    .set_node_bookmarked_status(&map_id(id_seed), value);
            }
            WeaveTransition::RemoveNode { id_seed } => {
                println!("weave.remove_node(&{});", map_id(id_seed));
                state.weave.remove_node(&map_id(id_seed));
            }
            WeaveTransition::RemoveNodeTracked { id_seed } => {
                println!(
                    "weave.remove_node_tracked(&{}, |_r| {{}});",
                    map_id(id_seed)
                );
                state.weave.remove_node_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::RemoveAllNodes { apply_seed } => {
                if apply_seed == 0 {
                    println!("weave.remove_all_nodes();");
                    state.weave.remove_all_nodes();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                println!("weave.metadata_mut(|m| *m = {});", content_seed);
                state.weave.metadata_mut(|m| *m = content_seed);
            }
            WeaveTransition::SortNodeChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_node_children_by(&{}, |a, b| {{ hash_value(a.id as u64 + {}).cmp(&hash_value(b.id as u64 + {})) }});",
                    map_id(id_seed),
                    sort_seed,
                    sort_seed
                );
                state.weave.sort_node_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortNodeChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_node_children_by_id(&{}, |a, b| {{ hash_value(*a as u64 + {}).cmp(&hash_value(*b as u64 + {})) }});",
                    map_id(id_seed),
                    sort_seed,
                    sort_seed
                );
                state
                    .weave
                    .sort_node_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    });
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_roots_by(|a, b| {{ hash_value(a.id as u64 + {}).cmp(&hash_value(b.id as u64 + {})) }});",
                    sort_seed, sort_seed
                );
                state.weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_roots_by_id(|a, b| {{ hash_value(*a as u64 + {}).cmp(&hash_value(*b as u64 + {})) }});",
                    sort_seed, sort_seed
                );
                state.weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_bookmarks_by(|a, b| {{ hash_value(a.id as u64 + {}).cmp(&hash_value(b.id as u64 + {})) }});",
                    sort_seed, sort_seed
                );
                state.weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                println!(
                    "weave.sort_bookmarks_by_id(|a, b| {{ hash_value(*a as u64 + {}).cmp(&hash_value(*b as u64 + {})) }});",
                    sort_seed, sort_seed
                );
                state.weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::MoveNode {
                id_seed,
                new_parents_seeds,
            } => {
                state.scratchpad_set.clear();

                let node_id = map_id(id_seed);
                if let Some(node) = state.weave.get_node(&node_id) {
                    for child in node.to() {
                        upwards_subgraph(state.weave.nodes(), child, &mut state.scratchpad_set);
                    }
                }

                let mut new_parents = Vec::with_capacity(new_parents_seeds.len());

                for id in new_parents_seeds.into_iter().map(&map_id) {
                    if !state.scratchpad_set.contains(&id) || !state.weave.contains(&id) {
                        new_parents.push(id);
                    }
                }

                println!("weave.move_node(&{}, &{:?});", map_id(id_seed), new_parents);
                state.weave.move_node(&map_id(id_seed), &new_parents);
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                println!(
                    "weave.get_contents_mut(&{}, |c| c.content_seed = {});",
                    map_id(id_seed),
                    content_seed % 4
                );
                state
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| c.content_seed = content_seed % 4);
            }
            WeaveTransition::SplitNode { id_seed, at_seed } => {
                let split_at = state
                    .weave
                    .get_node(&map_id(id_seed))
                    .map(|node| {
                        (at_seed
                            .checked_rem(node.contents.length)
                            .unwrap_or_default()) as usize
                    })
                    .unwrap_or_default();
                println!(
                    "weave.split_node(&{}, {}, {});",
                    map_id(id_seed),
                    split_at,
                    state.counter
                );
                state
                    .weave
                    .split_node(&map_id(id_seed), split_at, state.counter);
            }
            WeaveTransition::MergeNodeWithParent { id_seed } => {
                println!("weave.merge_with_parent(&{});", map_id(id_seed));
                state.weave.merge_with_parent(&map_id(id_seed));
            }
        }
        if state.weave.nodes().len() > old_node_count {
            state.counter += 1;
        }
        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}

// Copied from src/lib.rs
#[stacksafe]
fn add_node_identifiers<'a, K, N, T, S>(
    nodes: &'a impl Index<&'a K, Output = N>,
    id: &'a K,
    identifiers: &mut Vec<K>,
    identifier_set: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let node = nodes.index(id);

    if !identifier_set.contains(id)
        && node
            .from()
            .into_iter()
            .all(|parent| identifier_set.contains(parent))
    {
        identifiers.push(*id);
        identifier_set.insert(*id);
        for child in node.to().into_iter() {
            add_node_identifiers(nodes, child, identifiers, identifier_set);
        }
    }
}

// Copied from src/lib.rs
#[stacksafe]
fn downwards_subgraph<'a, K, N, T, S>(
    nodes: &'a impl Index<&'a K, Output = N>,
    id: &'a K,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let node = nodes.index(id);

    if identifiers.insert(*id) {
        for parent in node.from().into_iter() {
            downwards_subgraph(nodes, parent, identifiers);
        }
    }
}

// Copied from src/lib.rs
#[stacksafe]
fn upwards_subgraph<'a, K, N, T, S>(
    nodes: &'a impl Index<&'a K, Output = N>,
    id: &'a K,
    identifiers: &mut HashSet<K, S>,
) where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let node = nodes.index(id);

    if identifiers.insert(*id) {
        for child in node.to().into_iter() {
            upwards_subgraph(nodes, child, identifiers);
        }
    }
}

pub fn node_identifiers<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    ids: impl Iterator<Item = &'a K>,
) -> (Vec<K>, HashSet<K, S>)
where
    K: Hash + Copy + Eq + 'a,
    N: Node<K, T> + 'a,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    let mut identifiers = Vec::with_capacity(nodes.len());
    let mut identifier_set = HashSet::with_capacity_and_hasher(nodes.len(), S::default());
    for id in ids {
        if nodes.contains_key(id) {
            if let Some(node) = nodes.get(id) {
                for parent in node.from() {
                    identifier_set.insert(*parent);
                }
            }
            add_node_identifiers(nodes, id, &mut identifiers, &mut identifier_set);
            if let Some(node) = nodes.get(id) {
                for parent in node.from() {
                    identifier_set.remove(parent);
                }
            }
        }
    }
    (identifiers, identifier_set)
}

/*
#[test]
fn transition_set() {
    let items = vec![];

    let mut state = WeaveWrapper {
        weave: IndependentWeave::with_capacity(items.len(), items.len() as u32),
        counter: 0,
        scratchpad: Vec::with_capacity(items.len()),
    };
    for item in items {
        state = WeaveWrapper::apply(state, &vec![], item);
    }
}
*/
