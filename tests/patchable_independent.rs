use std::{
    hash::{BuildHasher, Hash, RandomState},
    iter,
};

use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use proptest::{collection::size_range, prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use universal_weave::{
    ActivePathWeave, BookmarkableWeave, DiscreteWeave, IndependentWeave as IndependentWeaveTrait,
    MetadataWeave, Node, SemiIndependentWeave, SortableBookmarkableWeave, SortableWeave, Weave,
    independent::{IndependentNode, IndependentWeave},
    wrappers::PatchablePathWeave,
};

const CASES: u32 = 4096;
const MAX_TRANSITIONS: usize = 512;

prop_state_machine! {
    #![proptest_config(Config {
        cases: CASES,
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
    type State = Vec<Self::Transition>;
    type Transition = (WeaveTransition, u32, u8);

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
    #[proptest(weight = 8)]
    Insert {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content: Vec<u8>,
    },
    #[proptest(weight = 4)]
    InsertWithChildren {
        filter_cycles: bool,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        to_seeds: Vec<u32>,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        from_seeds: Vec<u32>,
        active: bool,
        bookmarked: bool,
        content: Vec<u8>,
    },
    #[proptest(weight = 7)]
    SetActive {
        value: bool,
        id_seed: u32,
    },
    SetBookmarked {
        value: bool,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    Remove {
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    RemoveTracked {
        id_seed: u32,
    },
    Clear {
        apply_seed: u16,
    },
    MetadataMut {
        content_seed: u32,
    },
    SortChildrenBy {
        id_seed: u32,
        sort_seed: u32,
    },
    SortChildrenById {
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
    SetActivePath {
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=16), ()))")]
        id_seeds: Vec<u32>,
    },
    #[proptest(weight = 3)]
    MoveTo {
        filter_cycles: bool,
        #[proptest(strategy = "any_with::<Vec<u32>>((size_range(0..=3), ()))")]
        new_parents_seeds: Vec<u32>,
        id_seed: u32,
    },
    GetContentsMut {
        id_seed: u32,
        content: Vec<u8>,
    },
    #[proptest(weight = 3)]
    Split {
        at_seed: u32,
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    MergeWithParent {
        id_seed: u32,
    },
    #[proptest(weight = 3)]
    SplitAt {
        seed: u32,
    },
    #[proptest(weight = 3)]
    SplitOut {
        filter_invalid: bool,
        seed_a: u32,
        seed_b: u32,
    },
    #[proptest(weight = 3)]
    InsertAt {
        seed: u32,
        content: Vec<u8>,
    },
}

#[allow(clippy::type_complexity)]
struct WeaveWrapper {
    weave: PatchablePathWeave<
        IndependentWeave<u32, Vec<u8>, u32, RandomState>,
        u32,
        IndependentNode<u32, Vec<u8>, RandomState>,
        Vec<u8>,
    >,
    counter: u32,
    scratchpad: Vec<u32>,
    scratchpad_set: HashSet<u32>,
    active_content: Vec<u8>,
}

impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        WeaveWrapper {
            weave: PatchablePathWeave::new(IndependentWeave::with_capacity(
                ref_state.len(),
                ref_state.len() as u32,
            )),
            counter: 0,
            scratchpad: Vec::with_capacity(ref_state.len()),
            scratchpad_set: HashSet::with_capacity(ref_state.len()),
            active_content: Vec::with_capacity(ref_state.len()),
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

        match transition.0 {
            WeaveTransition::Insert {
                from_seeds,
                active,
                bookmarked,
                content,
            } => {
                let node = IndependentNode {
                    id: state.counter,
                    from: IndexSet::from_iter(from_seeds.into_iter().map(&map_id)),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: content,
                };
                state.weave.insert(node);
            }
            WeaveTransition::InsertWithChildren {
                filter_cycles,
                from_seeds,
                to_seeds,
                active,
                bookmarked,
                content,
            } => {
                let mut node = IndependentNode {
                    id: state.counter,
                    from: from_seeds.into_iter().map(&map_id).collect(),
                    to: to_seeds.into_iter().map(&map_id).collect(),
                    active,
                    bookmarked,
                    contents: content,
                };
                if filter_cycles {
                    state.scratchpad.clear();
                    state.scratchpad_set.clear();

                    for parent in node.from.iter().copied() {
                        ancestor_subgraph(
                            state.weave.nodes(),
                            parent,
                            &mut state.scratchpad,
                            &mut state.scratchpad_set,
                        );
                    }

                    state.scratchpad.clear();
                    state.scratchpad.extend(
                        node.to
                            .drain(..)
                            .filter(|id| !state.scratchpad_set.contains(id)),
                    );
                    node.to.extend(state.scratchpad.drain(..));
                }

                state.weave.insert(node);
            }
            WeaveTransition::SetActive { id_seed, value } => {
                state.weave.set_active(&map_id(id_seed), value);
            }
            WeaveTransition::SetBookmarked { id_seed, value } => {
                state.weave.set_bookmarked(&map_id(id_seed), value);
            }
            WeaveTransition::Remove { id_seed } => {
                state.weave.remove(&map_id(id_seed));
            }
            WeaveTransition::RemoveTracked { id_seed } => {
                state.weave.remove_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::Clear { apply_seed } => {
                if apply_seed == 0 {
                    state.weave.clear();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.weave.metadata_mut(|m| *m = content_seed);
            }
            WeaveTransition::SortChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_children_by_id(&map_id(id_seed), |a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SetActivePath { id_seeds } => {
                let active: Vec<u32> = id_seeds.into_iter().map(&map_id).collect();

                state.weave.set_active_path(active.into_iter());
            }
            WeaveTransition::MoveTo {
                filter_cycles,
                id_seed,
                new_parents_seeds,
            } => {
                let id = map_id(id_seed);
                let mut new_parents: Vec<u32> =
                    new_parents_seeds.into_iter().map(&map_id).collect();
                if filter_cycles && let Some(node) = state.weave.get(&id) {
                    state.scratchpad.clear();
                    state.scratchpad_set.clear();

                    for child in node.to().iter().copied() {
                        descendant_subgraph(
                            state.weave.nodes(),
                            child,
                            &mut state.scratchpad,
                            &mut state.scratchpad_set,
                        );
                    }

                    state.scratchpad.clear();
                    state.scratchpad.extend(
                        new_parents
                            .drain(..)
                            .filter(|id| !state.scratchpad_set.contains(id)),
                    );
                    new_parents.append(&mut state.scratchpad);
                }

                state.weave.move_to(&id, &new_parents);
            }
            WeaveTransition::GetContentsMut { id_seed, content } => {
                let _ = state
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| *c = content);
            }
            WeaveTransition::Split { id_seed, at_seed } => {
                let split_at = state
                    .weave
                    .get(&map_id(id_seed))
                    .map(|node| {
                        (at_seed
                            .checked_rem(node.contents.len() as u32)
                            .unwrap_or_default()) as usize
                    })
                    .unwrap_or_default();
                state.weave.split(&map_id(id_seed), split_at, state.counter);
            }
            WeaveTransition::MergeWithParent { id_seed } => {
                state.weave.merge_with_parent(&map_id(id_seed));
            }
            WeaveTransition::SplitAt { seed } => {
                state.active_content.clear();
                state
                    .active_content
                    .extend(state.weave.active_content().flatten().copied());

                let at = (seed
                    .checked_rem(state.active_content.len() as u32 + 2)
                    .unwrap_or_default()) as usize;

                state.weave.split_at(at, || {
                    state.counter += 1;
                    state.counter
                });

                assert!(
                    state.active_content.drain(..).eq(state
                        .weave
                        .active_content()
                        .flatten()
                        .copied())
                );
            }
            WeaveTransition::SplitOut {
                filter_invalid,
                seed_a,
                seed_b,
            } => {
                state.active_content.clear();
                state
                    .active_content
                    .extend(state.weave.active_content().flatten().copied());

                let start = (seed_a
                    .checked_rem(state.active_content.len() as u32 + 2)
                    .unwrap_or_default()) as usize;
                let end = (seed_b
                    .checked_rem(state.active_content.len() as u32 + 2)
                    .unwrap_or_default()) as usize;

                let range = if filter_invalid && start > end {
                    end..start
                } else {
                    start..end
                };

                if range.end >= range.start {
                    state.active_content.splice(range.clone(), iter::empty());
                }

                state.weave.split_out(range, || {
                    state.counter += 1;
                    state.counter
                });

                assert!(
                    state.active_content.drain(..).eq(state
                        .weave
                        .active_content()
                        .flatten()
                        .copied())
                );
            }
            WeaveTransition::InsertAt { seed, content } => {
                state.active_content.clear();
                state
                    .active_content
                    .extend(state.weave.active_content().flatten().copied());

                let at = (seed
                    .checked_rem(state.active_content.len() as u32 + 2)
                    .unwrap_or_default()) as usize;

                state.active_content.splice(at..at, content.iter().copied());

                state.weave.insert_at(at, content, || {
                    state.counter += 1;
                    state.counter
                });

                assert!(
                    state.active_content.drain(..).eq(state
                        .weave
                        .active_content()
                        .flatten()
                        .copied())
                );
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
fn ancestor_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id)
            && let Some(node) = nodes.get(&id)
        {
            scratchpad.extend(node.from().into_iter().rev().copied());
        }
    }
}

// Copied from src/lib.rs
fn descendant_subgraph<'a, K, N, T, S>(
    nodes: &'a HashMap<K, N, S>,
    id: K,
    scratchpad: &mut Vec<K>,
    identifiers: &mut HashSet<K>,
) where
    K: Hash + Copy + Eq + Ord + 'a,
    N: Node<K, T>,
    <N as Node<K, T>>::From: 'a,
    <N as Node<K, T>>::To: 'a,
    &'a N::From: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    &'a N::To: IntoIterator<Item = &'a K, IntoIter: DoubleEndedIterator>,
    S: BuildHasher + Default + Clone,
{
    scratchpad.push(id);

    while let Some(id) = scratchpad.pop() {
        if identifiers.insert(id)
            && let Some(node) = nodes.get(&id)
        {
            scratchpad.extend(node.to().into_iter().rev().copied());
        }
    }
}
