use std::hash::{BuildHasher, RandomState};

use indexmap::IndexSet;
use loro::Frontiers;
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use rkyv::{Archive, Deserialize, Serialize};
use universal_weave::{
    BookmarkableWeave, IndependentContents, MetadataWeave, SemiIndependentWeave,
    SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave, loro::DependentLoroWeave},
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
    AddNode {
        from_seed: Option<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
    },
    #[proptest(weight = 6)]
    SetNodeActiveStatus {
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
        apply_seed: u8,
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
    GetContentsMut {
        id_seed: u32,
        content_seed: u32,
    },
    Update,
    Commit,
    CommitAndRevert,
}

struct WeaveWrapper {
    weave: DependentLoroWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    last_commit: Option<Frontiers>,
    ordered_node_identifiers: Vec<u32>,
    ordered_node_identifiers_from: Vec<u32>,
    active_path: Vec<u32>,
    path_from: Vec<u32>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
struct WeaveContent(u32);

impl IndependentContents for WeaveContent {}

// Invariants are validated by the function's contracts
impl StateMachineTest for WeaveWrapper {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        WeaveWrapper {
            weave: DependentLoroWeave::try_from(DependentWeave::with_capacity(
                ref_state.len(),
                ref_state.len() as u32,
            ))
            .unwrap(),
            counter: 0,
            last_commit: None,
            ordered_node_identifiers: Vec::with_capacity(ref_state.len()),
            ordered_node_identifiers_from: Vec::with_capacity(ref_state.len()),
            active_path: Vec::with_capacity(ref_state.len()),
            path_from: Vec::with_capacity(ref_state.len()),
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
        let target = map_id(transition.1);

        match transition.0 {
            WeaveTransition::AddNode {
                from_seed,
                active,
                bookmarked,
                content_seed,
            } => {
                state.weave.add_node(DependentNode {
                    id: state.counter,
                    from: from_seed.map(map_id),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent(content_seed),
                });
            }
            WeaveTransition::SetNodeActiveStatus { id_seed, value } => {
                state.weave.set_node_active_status(&map_id(id_seed), value);
            }
            WeaveTransition::SetNodeBookmarkedStatus { id_seed, value } => {
                state
                    .weave
                    .set_node_bookmarked_status(&map_id(id_seed), value);
            }
            WeaveTransition::RemoveNode { id_seed } => {
                state.weave.remove_node(&map_id(id_seed));
            }
            WeaveTransition::RemoveNodeTracked { id_seed } => {
                state.weave.remove_node_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::RemoveAllNodes { apply_seed } => {
                if apply_seed == 0 {
                    state.weave.remove_all_nodes();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                state.weave.metadata_mut(|m| *m = content_seed)
            }
            WeaveTransition::SortNodeChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state.weave.sort_node_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortNodeChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                state
                    .weave
                    .sort_node_children_by_id(&map_id(id_seed), |a, b| {
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
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                let _ = state
                    .weave
                    .get_contents_mut(&map_id(id_seed), |c| c.0 = content_seed);
            }
            WeaveTransition::Update => {
                state.weave.update(|_doc| {}).unwrap();
            }
            WeaveTransition::Commit => {
                state
                    .weave
                    .update(|doc| {
                        doc.commit();
                        state.last_commit = Some(doc.state_frontiers());
                    })
                    .unwrap();
            }
            WeaveTransition::CommitAndRevert => {
                state
                    .weave
                    .update(|doc| {
                        doc.commit();
                        if let Some(last_commit) = &state.last_commit {
                            doc.revert_to(last_commit).unwrap();
                        }
                    })
                    .unwrap();
            }
        }
        assert!(state.weave.validate());
        if state.weave.nodes().len() > old_node_count {
            state.counter += 1;
        }

        if transition.2.is_multiple_of(4) {
            state
                .weave
                .get_ordered_node_identifiers(&mut state.ordered_node_identifiers);
            state.weave.get_ordered_node_identifiers_from(
                &target,
                &mut state.ordered_node_identifiers_from,
            );
            state.weave.get_active_path(&mut state.active_path);
            state.weave.get_path_from(&target, &mut state.path_from);
        }

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}
