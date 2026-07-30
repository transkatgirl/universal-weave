use std::hash::{BuildHasher, RandomState};

use hashbrown::HashMap;
use indexmap::IndexSet;
use loro::{ExportMode, PeerID, VersionVector};
use proptest::{prelude::*, strategy::Strategy, test_runner::Config};
use proptest_derive::Arbitrary;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};
use rkyv::{Archive, Deserialize, Serialize};
use universal_weave::{
    BookmarkableWeave, IndependentContents, MetadataWeave, SemiIndependentWeave,
    SortableBookmarkableWeave, SortableWeave, Weave,
    dependent::{DependentNode, DependentWeave, loro::DependentLoroWeave},
};

const CASES: u32 = 2048;
const MAX_TRANSITIONS: usize = 512;

prop_state_machine! {
    #![proptest_config(Config {
        cases: CASES,
        //verbose: 1,
        max_shrink_time: MAX_TRANSITIONS as u32 * 8000,
        max_shrink_iters: u32::MAX-1,
        //timeout: 1000,
        .. Config::default()
    })]

    #[test]
    fn run_state_machine(
        sequential
        1..MAX_TRANSITIONS
        =>
        VirtualPeers
    );
}

struct WeaveStateMachine;

impl ReferenceStateMachine for WeaveStateMachine {
    type State = Vec<VirtualPeerTransition>;
    type Transition = VirtualPeerTransition;

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
enum VirtualPeerTransition {
    #[proptest(weight = 8)]
    PeerA(WeaveTransition),
    #[proptest(weight = 8)]
    PeerB(WeaveTransition),
    SyncAtoB,
    SyncBtoA,
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
    GetActivePath,
    GetPathFrom {
        id_seed: u32,
    },
    #[proptest(weight = 8)]
    AddNode {
        from_seed: Option<u32>,
        active: bool,
        bookmarked: bool,
        content_seed: u32,
    },
    #[proptest(weight = 4)]
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
}

#[derive(Default)]
struct VirtualPeers {
    a: WeaveWrapper,
    b: WeaveWrapper,
}

struct VirtualPeerMessage {
    id: PeerID,
    data: Vec<u8>,
    version: VersionVector,
}

struct WeaveWrapper {
    id: PeerID,
    weave: DependentLoroWeave<u32, WeaveContent, u32, RandomState>,
    counter: u32,
    scratchpad: Vec<u32>,
    peers: HashMap<PeerID, VersionVector>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
struct WeaveContent {
    length: u32,
}

impl Default for WeaveWrapper {
    fn default() -> Self {
        let mut weave =
            DependentLoroWeave::try_from(DependentWeave::with_capacity(MAX_TRANSITIONS, 0))
                .unwrap();

        Self {
            id: weave.update(|doc| doc.peer_id()).unwrap(),
            weave,
            counter: 0,
            scratchpad: Vec::with_capacity(MAX_TRANSITIONS),
            peers: HashMap::new(),
        }
    }
}

impl WeaveWrapper {
    fn apply(&mut self, transition: WeaveTransition) {
        let s = RandomState::default();
        let hash_value = |value: u64| s.hash_one(value);
        let map_id = |seed: u32| seed % (self.counter + 2);
        let old_node_count = self.weave.nodes().len();

        match transition {
            WeaveTransition::GetOrderedNodeIdentifiers { reversed } => {
                if reversed {
                    self.weave
                        .get_ordered_node_identifiers_reversed_children(&mut self.scratchpad);
                } else {
                    self.weave
                        .get_ordered_node_identifiers(&mut self.scratchpad);
                }
            }
            WeaveTransition::GetOrderedNodeIdentifiersFrom { id_seed, reversed } => {
                if reversed {
                    self.weave
                        .get_ordered_node_identifiers_from_reversed_children(
                            &map_id(id_seed),
                            &mut self.scratchpad,
                        );
                } else {
                    self.weave
                        .get_ordered_node_identifiers_from(&map_id(id_seed), &mut self.scratchpad);
                }
            }
            WeaveTransition::GetActivePath => self.weave.get_active_path(&mut self.scratchpad),
            WeaveTransition::GetPathFrom { id_seed } => self
                .weave
                .get_path_from(&map_id(id_seed), &mut self.scratchpad),
            WeaveTransition::AddNode {
                from_seed,
                active,
                bookmarked,
                content_seed,
            } => {
                self.weave.add_node(DependentNode {
                    id: self.counter,
                    from: from_seed.map(map_id),
                    to: IndexSet::default(),
                    active,
                    bookmarked,
                    contents: WeaveContent {
                        length: content_seed % 64,
                    },
                });
            }
            WeaveTransition::SetNodeActiveStatus { id_seed, value } => {
                self.weave.set_node_active_status(&map_id(id_seed), value);
            }
            WeaveTransition::SetNodeBookmarkedStatus { id_seed, value } => {
                self.weave
                    .set_node_bookmarked_status(&map_id(id_seed), value);
            }
            WeaveTransition::RemoveNode { id_seed } => {
                self.weave.remove_node(&map_id(id_seed));
            }
            WeaveTransition::RemoveNodeTracked { id_seed } => {
                self.weave.remove_node_tracked(&map_id(id_seed), |_r| {});
            }
            WeaveTransition::RemoveAllNodes { apply_seed } => {
                if apply_seed == 0 {
                    self.weave.remove_all_nodes();
                }
            }
            WeaveTransition::MetadataMut { content_seed } => {
                self.weave.metadata_mut(|m| *m = content_seed)
            }
            WeaveTransition::SortNodeChildrenBy { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_node_children_by(&map_id(id_seed), |a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortNodeChildrenById { id_seed, sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave
                    .sort_node_children_by_id(&map_id(id_seed), |a, b| {
                        hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                    });
            }
            WeaveTransition::SortRootsBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_roots_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortRootsById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_roots_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksBy { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_bookmarks_by(|a, b| {
                    hash_value(a.id as u64 + sort_seed).cmp(&hash_value(b.id as u64 + sort_seed))
                });
            }
            WeaveTransition::SortBookmarksById { sort_seed } => {
                let sort_seed = sort_seed as u64;
                self.weave.sort_bookmarks_by_id(|a, b| {
                    hash_value(*a as u64 + sort_seed).cmp(&hash_value(*b as u64 + sort_seed))
                });
            }
            WeaveTransition::GetContentsMut {
                id_seed,
                content_seed,
            } => {
                self.weave
                    .get_contents_mut(&map_id(id_seed), |c| c.length = content_seed % 64);
            }
            WeaveTransition::Update => {
                self.weave.update(|_doc| {}).unwrap();
            }
            WeaveTransition::Commit => {
                self.weave
                    .update(|doc| {
                        doc.commit();
                    })
                    .unwrap();
            }
        }
        assert!(self.weave.validate());
        if self.weave.nodes().len() > old_node_count {
            self.counter += 1;
        }
    }
    fn import(&mut self, message: VirtualPeerMessage) {
        self.weave
            .update(|doc| {
                doc.import(&message.data).unwrap();
            })
            .unwrap();

        self.peers.insert(message.id, message.version);

        assert!(self.weave.validate());
    }
    fn export(&mut self, peer: PeerID) -> VirtualPeerMessage {
        self.weave
            .update(|doc| VirtualPeerMessage {
                id: self.id,
                data: if let Some(version) = self.peers.get(&peer) {
                    doc.export(ExportMode::updates(version))
                } else {
                    doc.export(ExportMode::all_updates())
                }
                .unwrap(),
                version: doc.oplog_vv(),
            })
            .unwrap()
    }
}

impl IndependentContents for WeaveContent {}

// Invariants are validated by the function's contracts
impl StateMachineTest for VirtualPeers {
    type SystemUnderTest = Self;
    type Reference = WeaveStateMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        VirtualPeers::default()
    }
    fn apply(
        mut state: Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        match transition {
            VirtualPeerTransition::PeerA(transition) => {
                state.a.apply(transition);
            }
            VirtualPeerTransition::PeerB(transition) => {
                state.b.apply(transition);
            }
            VirtualPeerTransition::SyncAtoB => {
                state.b.import(state.a.export(state.b.id));
                state.a.import(state.b.export(state.a.id));
                assert_eq!(state.a.weave.as_weave(), state.b.weave.as_weave());
            }
            VirtualPeerTransition::SyncBtoA => {
                state.a.import(state.b.export(state.a.id));
                state.b.import(state.a.export(state.b.id));
                assert_eq!(state.a.weave.as_weave(), state.b.weave.as_weave());
            }
        }

        state
    }
    fn check_invariants(
        _state: &Self::SystemUnderTest,
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
    }
}
