# Verification

`DependentWeave` and `IndependentWeave` are formally verified with [Creusot](https://github.com/creusot-rs/creusot). The contracts their methods used to check at runtime (the `#[contract(...)]` annotations in debug builds) are now Creusot specifications in `src/dependent/mod.rs` and `src/independent/mod.rs`, proven for all inputs. `PatchablePathWeave` is verified over `IndependentWeave`: its patch operations are proven to edit the text of the active path as documented (see [`PatchablePathWeave`](#patchablepathweave)).

## Running

Verification needs Creusot `0.15.0-dev` at commit [`fe19159`](https://github.com/creusot-rs/creusot/commit/fe191590ea65beae627979cc2197fe42619548a2) (2026-10-24), its `nightly-2026-08-03` toolchain, and its `cargo creusot` command. `Cargo.toml` depends on the matching `creusot-std = "0.15.0-dev"`, which isn't published on crates.io: it must come from that Creusot checkout, for example with a `[patch.crates-io]` entry in `.cargo/config.toml`:

```toml
[patch.crates-io]
creusot-std = { path = "/path/to/creusot/creusot-std" }
```

Then run:

```sh
cargo creusot -- --no-default-features
```

This should end with `Proved (740 files) ✔`. The `rkyv`, `serde` and `loro` features can't be enabled: Creusot can't process their derives and implementations, and the build stops with an error saying so. The `layout` module, the wrappers other than `PatchablePathWeave` and the `legacy` module aren't compiled while Creusot is running.

`cargo creusot` searches for proofs with [why3find](https://git.frama-c.com/pub/why3find), which it configures in `why3find.json` and which caches the proofs it finds in `verif/` (both are ignored by git). The proofs were found with Alt-Ergo 2.6.4, Z3 5.1.0 and CVC5 1.4.1, and with `"time": 5.0` in `why3find.json`. To prove a single function or lemma, pass its path under `verif/`, such as `'independent/impl_DiscreteWeave_for_IndependentWeave_K_T_M_S/split'` or `'independent/verification/edit/*'`. `--why3find-arg=-X` shows the goals that fail, and `--why3find-arg=-f` searches for proofs again instead of replaying the cached ones.

Outside of Creusot, the specifications and proofs are removed at compile time: `creusot-std`'s macros expand to nothing (or to zero-sized ghost values), so builds compile the same code with or without it.

## What is proven

For every method of `DependentWeave`, `DependentNode`, `IndependentWeave` and `IndependentNode`:

- Each clause of the method's former contract holds. The clauses translate as follows:

  | Contract | Specification |
  | --- | --- |
  | `invariant(self.validate())` | the type invariant `self@.valid()` (`impl Invariant`), which holds on entry and must hold again for `^self` |
  | `!ret \|\| P` | `result ==> P` |
  | `old(e)` | `e` evaluated on `self@`, while `(^self)@` is the weave after the call |
  | `old(self.nodes.keys().copied().collect::<HashSet<_>>()) == self.nodes.keys().copied().collect::<HashSet<_>>()` | `(^self)@.same_keys(self@)` |
  | `old(self.roots.clone()) == self.roots` (`IndexSet` equality ignores order) | `same_elements((^self)@.roots, self@.roots)` |
  | `self.active.contains(id)` (`IndependentWeave`) | `self@.active.contains(&id.deep_model())` |
  | `self.nodes[id].to.iter().any(\|c\| self.active.contains(c))` | `has_child_in(self@.nodes, self@.active, id.deep_model())` |

  Identifiers are compared through their `DeepModel` (`Id<K>`), maps are modeled as `FMap`s, hash sets as `FSet`s, and index sets as sequences without duplicates.

- `valid()` mirrors `validate()` check by check. For `DependentWeave` (in `src/dependent/verification.rs`), its acyclicity check says that some rank function strictly increases from parent to child. For `IndependentWeave` (in `src/independent/verification/mod.rs`), `valid()` is `valid_structure()`, which includes the same acyclicity check, and `forms_path`: the active nodes are exactly the nodes of a path from some node up to a root.
- `validate()` returns exactly `valid()`. In particular, `detect_cycles` and `valid_topology` report a cycle exactly when the links form one, which the serde `Deserialize` implementations rely on.
- The method never panics: no failed `unwrap`, no out-of-bounds access, no failure of the `assert!`s in `split` and `merge_with_parent`, and no arithmetic overflow, including where the code is marked "Can never overflow" or "Can never underflow".

Creusot proves partial correctness: if a method returns, it returns a correct result. Termination is only proven for the functions marked `#[check(terminates)]`:

- in both weaves, the getters (`len`, `is_empty`, `nodes`, `roots`, `contains`, `contains_active`, `get`, `get_parents`, `get_children`, `get_contents`, `metadata`, `bookmarks`, `contains_bookmark` and `active`), `reserve`, `shrink_to_fit`, `clear`, `set_bookmarked` and `validate`, with every function `validate` calls, such as `detect_cycles`, `valid_topology` and `longest_candidate_path_to_root`, so validating a deserialized weave always ends;
- `DependentWeave`'s `insert`, `set_active`, `get_ordered_identifiers`, `get_ordered_identifiers_from`, `get_active_path` and `get_path_from`;
- the nodes' getters, `new` and `validate`.

Elsewhere, Creusot only checks that loops keep their invariants, not that they end. Methods which call a callback, a comparator or a `DiscreteContents` method can't be marked, since nothing says those end, and neither can `new` and `with_capacity`, which call `S::default()`. `IndependentWeave`'s other methods run its graph searches, which aren't marked.

Methods which take a callback (`remove_tracked`, `metadata_mut`, `get_contents_mut` and the `sort_*` methods) require that the callback accepts every value it can be given. Closures without a precondition, which is every closure written outside of Creusot, meet this. The requirement is stated on the trait methods in `src/lib.rs`, since an implementation can't require more than its trait.

What the callbacks are given is proven too. `metadata_mut` and `get_contents_mut` call theirs on the metadata or on the node's contents, which keep what the callback leaves there, and return what it returns (`postcondition_once`). `get_contents_mut` drops its callback without calling it if there is no such node. `remove_tracked` calls `on_removal` once with each removed node, as it was before the removal (`fed`: the callback goes through a sequence of states, each reached from the previous one by a call, and the last one is dropped). For `DependentWeave`, the nodes come in the order of a traversal of the subtree.

The `sort_*` methods sort stably by their comparator whenever it behaves as a total order: if, in every state it can reach, `cmp` compares its arguments as some total preorder `le` orders them (`compares_by`, with `le` over nodes for the `sort_*_by` methods and over identifiers for the `sort_*_by_id` methods), the sorted sequence is a permutation of the original in which each element is at most the ones after it under `le`, and equivalent elements keep their order (`sorted_stably`). Nothing is promised about the order for other comparators.

`DiscreteContents::split` and `merge` carry the contracts their documentation states: a failed split returns the original contents, and a failed merge returns both originals in order. Implementations compiled while Creusot is running must prove them; the `()` and `Vec<u8>` implementations do. The weaves' `split` and `merge_with_parent` use these contracts to show that a failed split or merge changes nothing.

`split` and `merge` also promise `self.splits(at, result)` and `self.merges(value, result)`: two logic methods of `DiscreteContents`, which only exist while Creusot is running, so that implementations can say what they return. By default they promise nothing. The `()` and `Vec<u8>` implementations override them to say exactly what they return (for `Vec<u8>`, the two halves of the bytes at `at`, or the concatenation). Through them, the weaves' `split` and `merge_with_parent` say what the nodes contain afterwards, and that when they fail although the nodes could be split or merged, the contents refused.

`IndependentWeave`'s private `update_node_activity_in_place` and `fix_orphaned_activations` ensured `self.validate()` without requiring it. They are called while the weave is being changed, so they take the weave's fields instead of the weave, and require `structure(...)` (the weave is valid except that its active nodes may not form a path) and, for the former, `ready_to_activate`.

Clauses commented "Beyond the contract" are proven in addition to the former contracts:

| Method | Additional guarantee |
| --- | --- |
| `DependentWeave::insert` | Succeeds exactly when the node isn't its own parent, has no children, has a new identifier, and its parent (if any) exists. Stores the node as given, and adds only its identifier. Its parent gains it as its last child, a root node becomes the last root and a bookmarked node the last bookmark; nothing else changes. |
| `DependentWeave::remove`, `remove_tracked` | Remove exactly the subtree rooted at `id`, and update the active node as `active_after_removal` describes. `remove` returns the removed node, and `remove_tracked` passes each removed node to `on_removal`, in the order of a traversal of the subtree (see above). The roots, the bookmarks and each node's children keep their order, and a failed removal changes nothing. |
| `IndependentWeave::insert` | Succeeds exactly when the node can be linked in (`insertable`): it is consistent, it is new, its parents and children exist, and none of its children is one of its parents or their ancestors. A failed insertion changes nothing. A successful one adds only the node's identifier and links it as `inserted` describes: the node keeps its contents, each parent gains it as its last child and each child as its last parent, nothing else changes, a bookmarked node becomes the last bookmark, and the roots keep their order, with the node taking the place of its first root child, or added at the end if it is a root without root children. |
| `IndependentWeave::remove`, `remove_tracked` | Remove exactly the node and the nodes left without parents (`removal`, which determines them: `lemma_removal_unique`), and nothing else changes (`removal_shape`): the other nodes keep their contents and bookmarks, and their parents and children, the roots and the bookmarks lose the removed nodes and keep their order. `remove` returns the removed node, `remove_tracked` passes each removed node to `on_removal` once (see above), and a failed removal changes nothing. |
| `IndependentWeave::move_to` | Succeeds exactly when the node and every new parent exist and no new parent is the node or one of its descendants (`not_below`). A failed move changes nothing. A successful one relinks the node as `moved` describes: no contents change, new parents gain the node as their last child, other nodes keep their children in order, and the roots keep their order, with the node added at the end if it becomes a root. The node's new parents are `new_parents` in order, without repetitions (`dedup`). |
| `IndependentWeave::get_path_from` | The path holds every active node which `id` descends from, after all of its inactive nodes: it leads from `id` through inactive ancestors to the deepest active one, and then follows the active path to its root. Its inactive part is a shortest one: no path from `id` up to its first active node, or to a root if none of its nodes is active, has fewer nodes (`descending_to`). |
| `DependentWeave::get_ordered_identifiers` | Outputs the pre-order traversal of the roots' subtrees, in order: each node, followed by the traversals of its children's subtrees, in order (`preorder`). |
| `get_ordered_identifiers_from` | Outputs exactly `id` and its descendants, each after those of its parents which are listed (`traversal` for `DependentWeave`). For `DependentWeave`, the output is the pre-order traversal of the subtree rooted at `id` (`preorder`). |
| `get_ordered_identifiers`, `get_ordered_identifiers_from`, `get_active_path`, `get_path_from` | Leave the weave unchanged. |
| `split` | A successful split has the shape `split_shape` describes: the node keeps its parents, flags and position, and gets the new node as its only child, which takes its former children, in order, and no other node's contents change. For `IndependentWeave`, each of those children keeps its parents in order, with the new node in the place of the old one. The two nodes hold what the contents' `split` returned (`splits`). A failed split changes nothing, and if the nodes allowed it, the contents' `split` failed. |
| `merge_with_parent` | A successful merge has the shape `merge_shape` describes: the parent, which had no other children, takes the node's children, in order, and its bookmark, in the node's place among the bookmarks if the parent wasn't bookmarked, and no other node's contents change. For `IndependentWeave`, each of those children keeps its parents in order, with the parent in the place of the node. The parent holds what the contents' `merge` returned (`merges`). A failed merge changes nothing, and if the nodes allowed it, the contents' `merge` failed. |
| `set_bookmarked` | Changes nothing but the node's bookmark. A new bookmark comes last, and the others keep their order (`bookmark_set`). |
| `get_contents_mut` | Changes nothing but the node's contents, which are what the callback leaves them as (see above). |
| `metadata_mut` | Changes nothing but the metadata, which is what the callback leaves it as, and returns what the callback returns. |
| `new`, `with_capacity` | Store the given metadata. |
| Every other method taking `&mut self` | Leaves the metadata unchanged. |
| `DependentNode::new`, `IndependentNode::new` (`BuildableNode`) | Build the node from their arguments, not bookmarked. |
| `sort_children_by`, `sort_children_by_id` | Change nothing but the order of the node's children. |
| `sort_*` | Sort stably when the comparator behaves as a total order (see above). |
| `IndependentWeave::set_active`, `set_active_dependent_semantics`, `set_active_path` | Leave the roots and bookmarks unchanged, in order. |

Methods without a former contract have specifications too. `new` and `with_capacity` return an empty, valid weave, `reserve` and `shrink_to_fit` leave the weave unchanged, and the getters say what they return.

## Activation

Beyond keeping the active state valid, the proofs state which nodes each method makes active. These clauses are also commented "Beyond the contract", and the definitions they use are in `src/dependent/verification.rs` and `src/independent/verification/mod.rs`.

### `DependentWeave`

| Method | Active node afterwards |
| --- | --- |
| `set_active(id, true)` | `id` |
| `set_active(id, false)` | `id`'s parent if `id` was the active node, and unchanged otherwise (`active_after_set`) |
| `insert(node)` | `node` if `node.active`, and unchanged otherwise |
| `remove`, `remove_tracked` | `id`'s parent if the active node was in the removed subtree, and unchanged otherwise (`active_after_removal`) |
| `merge_with_parent(id)` | the merged node if `id` was the active node, and unchanged otherwise |
| `split` and every other method | unchanged |

`set_active` changes nothing but which node is active (`same_graph`).

### `IndependentWeave`

When activation recomputes the active path, it compares candidate paths by how many formerly active nodes they keep, and then by how few nodes they have in total (`no_better`). Starting from the active nodes `before`:

- Activating an inactive node `id` (`activated`): if `id` extends the active path (one of its parents ends the path, or `id` is a root and no node is active), `id` is added to it. Otherwise, the new active path is one of the best paths from `id` up to a root (`best_path_up`), extended below `id` as `kept_below` describes: by nothing if no path down from `id` reaches a formerly active node, and otherwise by one of the best paths from `id` down to a leaf (`best_path_down`), cut after its last formerly active node.
- Activating with `DependentWeave`'s semantics (`activated_at_end`) makes `id` the end of the active path: nothing changes if it already is, `id` is added if it extends the path, and otherwise the new active path is one of the best paths from `id` up to a root.
- Deactivating or removing nodes (`longest_path_within`): the new active path is one of the longest paths to a root through the nodes which stay active. When those nodes already form a path to a root, as when the node at the end of the path is deactivated, they all stay active.

| Method | Active nodes afterwards |
| --- | --- |
| `set_active(id, value)` | unchanged if `id` already is or isn't active as `value` says; `activated` when activating `id`; when deactivating it, a longest path through the other formerly active nodes |
| `set_active_dependent_semantics(id, true)` | `activated_at_end` |
| `set_active_dependent_semantics(id, false)` | without `id` if it ended the active path, and unchanged otherwise |
| `insert(node)` | `activated`, in the new graph, if `node` is active or becomes the parent of an active root (`inserted_active`), and unchanged otherwise |
| `move_to(id, new_parents)` | `activated`, in the new graph and from the other active nodes, if `id` is active and its parents change, and unchanged otherwise |
| `remove`, `remove_tracked` | a longest path through the formerly active nodes which remain |
| `split(id, _, new_id)` | with `new_id` too if `id` is active and has an active child, and unchanged otherwise |
| `merge_with_parent(id)` | without `id`, whose parent was active if `id` was |

| `set_active_path(active)` | a longest path to a root through the nodes which `active` lists, iterating over it until it ends (`iterates`) |

`set_active`, `set_active_dependent_semantics` and `set_active_path` change nothing but which nodes are active (`same_graph`).

When several paths are equally good, the specifications don't say which one is chosen: that depends on the order of each node's parents and children.

These guarantees rest on the following proofs about the code which finds the paths:

- `best_scored` returns a parent or child with the best score.
- The two scoring passes of `update_node_activity_in_place` and `set_active_dependent_semantics` record, for each node, the score of a best path from it up to a root or down to a leaf, and the parent or child which that path continues to (`up_scored` and `down_scored`). The walks along the recorded parents and children therefore follow best paths.
- `longest_candidate_path_to_root` returns a longest path to a root through its candidates, given a topological order which lists each candidate after its candidate parents. `fix_orphaned_activations` passes it one which lists every ancestor of an active node.
- `shortest_path_to_ancestor` returns a shortest path to a target: its breadth-first search finds nodes by increasing level, each one level above the node it was found from (`bfs_found` and `bfs_expanded`), so no descent from a target is shorter than the first one it visits (`lemma_bfs_shortest`).

## `PatchablePathWeave`

`PatchablePathWeave` is verified over `IndependentWeave`: `PatchablePathWeave<IndependentWeave<K, T, M, S>, K, IndependentNode<K, T, S>, T>`. Its specifications are in `src/wrappers/patchable.rs` and its definitions and lemmas in `src/wrappers/patchable/verification.rs`.

### The text of the active path

The patch operations edit the active path's contents as a sequence of units, so the contents need a model of units:

- `DiscreteContents` gained, while Creusot is running, a type `Unit` and a logic method `units()`: the units the contents are made of, in order. `len` returns `units().len()` and `is_empty` whether it is 0; these are now contracts of the trait, which the `()` and `Vec<u8>` implementations prove (`Vec<u8>`'s units are its bytes).
- `active_line(view)` is the active path read from its root down, and `active_text(view)` the units of its nodes' contents, in order (`text`).
- `spliced(t, start, end, x)` is `t` with the units from `start` to `end` replaced by `x`, as `Vec::splice` would, except that `start` and `end` are clamped to `t`'s length and a range which ends before it starts is empty.

| Method | Active path's text afterwards |
| --- | --- |
| `split_out(range, _)` | `spliced(text, range.start, range.end, [])` |
| `insert_at(at, contents, _, _)` | `spliced(text, at, at, contents.units())` |
| `replace(range, contents, _, _)` | `spliced(text, range.start, range.end, contents.units())` |
| `split_at(at, _)` | unchanged, and when it returns `true`, the text of some of the path's first nodes ends at `at`, or at the end of the text if `at` is beyond it |

These are the properties the `patchable_independent` test suite checks, for every `T` which meets the requirements below. The patch operations also remove no node and leave the metadata unchanged. `split_at` changes nothing when it returns `false`, which only happens when the contents refuse to split or `generate_id` returns an identifier already in the weave. `active_path_vec` returns the active path from its end up (its reverse is `active_line`), `path_vec(id)` what `get_path_from` returns, and `inner_scratchpad_mut` calls its callback on the weave and an empty vector, returning what it returns. None of them panics when their preconditions hold.

### Requirements

The wrapper's documented requirements become preconditions or are proven:

- Requirements 1 (*`IndependentWeave`-like activation semantics*) and 2 (*`insert`, `split` and `move_to` must never refuse a structurally valid operation*) are proven for `IndependentWeave`, from its specifications: each insertion, split and move the wrapper makes is shown to succeed, and each active path it ends with is derived from `set_active_path`'s, `set_active`'s or `insert`'s specification. When an active node is inserted inside the active path (`insert_at` at a node boundary), the rerouted path is shown to be exactly the old path with the node inserted (`lemma_activated_between`): no best path up or down from the node makes a detour.
- Requirement 3 (*`T::split()` must always split at the exact specified index and should never refuse a structurally valid operation*) is the precondition `exact_splits::<T>() && total_splits::<T>()`, stated with `DiscreteContents::splits`: splitting strictly inside contents always succeeds, and gives their units before and after the index. `split_at` only requires `exact_splits`, and returns `false` if the contents refuse.
- Requirement 4 (*`T::default()` must have a length of zero*) can't be stated, since `Default::default` has no specification, so the trusted helper `default_contents` assumes it (see [Trusted](#trusted)).

The methods which call `generate_id` require `generates(generate_id, nodes)`: it can always be called, it never returns an identifier of the weave, and no two calls return the same one, which is what "may panic if `generate_id` returns an identifier already in the Weave" excludes. `split_at` only requires `callable(generate_id)`. The methods which walk the active path also require its text to have at most `usize::MAX` units, since they add up the lengths of its nodes' contents with `strict_add`, which contents held in memory always do.

### Delegation

The trait implementations (`Weave`, `MetadataWeave`, `BookmarkableWeave`, `SortableWeave`, `SortableBookmarkableWeave`, `ActiveSingularWeave`, `ActivePathWeave`, `IndependentWeave`, `SemiIndependentWeave` and `DiscreteWeave`), `Default`, `From`, `AsRef`, `new`, `into_inner`, `as_inner` and `inner_scratchpad_mut` are verified for any weave `W`: they delegate to it, and meet the traits' contracts. The methods which take a callback restate their trait's precondition on it, as `IndependentWeave`'s and `DependentWeave`'s do, since an implementation which states no precondition doesn't take its trait's. Their behavior over `IndependentWeave` is that of `IndependentWeave`'s methods.

## Layout

| Path | Contents |
| --- | --- |
| `src/dependent/mod.rs`, `src/independent/mod.rs` | The code, with each method's specification, loop invariants, ghost state, and calls to lemmas |
| `src/dependent/verification.rs` | Definitions used by `DependentWeave`'s specifications (its view, `valid()`, subtrees, depth-first traversals, the pre-order traversal `preorder`) and the lemmas about them |
| `src/independent/verification/mod.rs` | Definitions used by `IndependentWeave`'s specifications: its view, `valid()`, `structure`, paths, and the lemmas about changing only which nodes are active or bookmarked |
| `src/independent/verification/*.rs` | Lemmas, by area: `graph` (closed sets, topological orders, candidate chains, descent), `search`, `kahn` and `paths` (the graph searches, including the levels of the breadth-first search), `traversal` (their results, as used by the getters), `edit` (`split`, `merge_with_parent`, `insert`, `move_to`), `removal` (what a removal deletes, and what it leaves), `activation` (recomputing the active path), `routes` (the paths which activation chooses: counting and comparing them, the scores of the best paths up and down, and the longest paths) and `validation` (`valid_topology`) |
| `src/independent/graph/` | The graph searches `IndependentWeave` is built on: depth-first searches (`search.rs`), topological sorts (`kahn.rs`) and path finding (`paths.rs`) |
| `src/independent/support.rs` | `validate` and the helpers which `IndependentWeave`'s methods share |
| `src/verification/mod.rs` | Definitions and lemmas shared by both weaves: identifier models, sequences without duplicates, the order of bookmarks and replaced elements, the calls of callbacks (`calls`, `fed`), iterating until the end (`iterates`), and facts about sequences and sets |
| `src/verification/specs/` | Trusted specifications of the external functions which the code calls |
| `src/wrappers/patchable.rs` | `PatchablePathWeave`, with its patch operations' specifications, loop invariants and proofs, and the helpers they share (`locate_range`, `split_bounds`, `set_active_line` and `insert_tail_node`) |
| `src/wrappers/patchable/verification.rs` | Definitions used by `PatchablePathWeave`'s specifications (lines, their text, `spliced`, the requirements on the contents and the identifiers) and the lemmas about them: the active path's lines, splitting, cutting and inserting into them, and which nodes activation keeps when a node is inserted into the active path |

The `verification` modules only exist while Creusot is running.

## Annotations

Proofs are kept out of the code where possible: in `src/dependent/mod.rs` and `src/independent/mod.rs`, proof code mostly calls lemmas, takes snapshots of ghost state, or states the facts which a lemma needs.

- Functions are specified with `#[requires(...)]` and `#[ensures(...)]`, and loops with `#[invariant(...)]`. Outside of Creusot, these attributes expand to nothing.
- `proof_assert!(...)` states a fact for the prover, `snapshot!(...)` takes a snapshot of ghost state, and `proof! { ... }` (defined in `src/lib.rs`) contains `let` bindings of snapshots, which only exist while Creusot is running.
- The graph searches take ghost arguments of type `Predicate<K>`: snapshots of the spec version of their filter. Outside of Creusot, these are zero-sized.
- Identifier types are bounded by `Verifiable`, which is `DeepModel` while Creusot is running and is implemented for every type otherwise, so the bound doesn't restrict users of the crate.

When changing either weave or `PatchablePathWeave`, update the specifications and invariants along with the code, run verification, and put any new proofs in the `verification` modules.

Some of Creusot's restrictions shape the proofs:

- Specifications can't mention a value while part of it is mutably borrowed. Methods which borrow the scratchpad or a node while their proofs need the rest of the weave destructure it first (`let Self { nodes, roots, active, scratchpad, .. } = self;`), or take snapshots beforehand.
- A `&mut self` method's type invariant is checked as soon as `self` is no longer used, which may be before the end of the method. Proofs about the final weave are therefore stated about a snapshot of it before its last change (as in `split`), or through a lemma which applies to any weave of the right shape (as in `set_bookmarked`, with `lemma_rebookmarked`).
- Two closures with the same body are different functions to the provers. A predicate passed to several functions is bound once (`let admitted = snapshot!(...)`), using `member_of(set)` rather than `|k| set.contains(&k)`.
- A lemma is only applied where the provers find a reason to, and some lemmas' conclusions don't give them one. Such lemmas are wrapped in boolean facts (like `closure_ancestor_ok` and the `*_ok` functions in `activation.rs`), which are applied wherever the fact is asserted.
- Each `ensures` of a lemma becomes a separate axiom over all of the lemma's arguments, and calling the lemma doesn't instantiate it (the call becomes `let _ = lemma(..) in ..`, which the provers drop): they apply the axiom by matching its terms against the goal's. An `ensures` which doesn't mention an argument that a `requires` constrains, or only mentions it in arithmetic, therefore can't be applied. Such conclusions go in a separate lemma (as `lemma_split_line_at` and `lemma_split_line_suffix` are split, and `lemma_insert_keeps_line` is split from `lemma_insert_between`), or restate a precondition which mentions the argument (as `lemma_insert_keeps_line` and `lemma_insert_active_after` do).
- A lemma whose precondition is quantified is hard to apply in a large proof, since the provers must re-prove the quantified formula where they apply it. `PatchablePathWeave`'s lemmas wrap such preconditions in named predicates (`ranked_above`, `below_end`, `ranked_under` and `within`) which the caller asserts first. For the same reason, the patch operations derive the active path from `set_active_path`'s existential postcondition inline: they prove a quantified implication, which the provers instantiate with its witnesses.
- Facts a lemma establishes inside a `proof_assert! { lemma(...); fact }` block only outlast the block if `fact` states them, and a `proof_assert!` nested inside another one is checked but not assumed afterwards.
- The provers check what follows a branch once for each way through the branches before it, so the number of goals of a long method grows with the product of its branches. `split_out` and `replace` call `locate_range`, `split_bounds` and `set_active_line`, whose contracts state what the callers need, instead of branching on where the range lies and reasoning about `set_active_path`'s existential postcondition themselves. The definitions those contracts use (`located`, `bounded` and `kept_line`) are only open inside `src/wrappers/patchable/verification.rs`, and lemmas there (`lemma_located`, `lemma_bounded` and `lemma_kept_line`) give them where they are needed, so that the callers' provers don't unfold them. A snapshot defined with `if` is best avoided for the same reason: the provers handle the conditional term poorly, which is why `kept_line` exists.

## Trusted

Verification proves the code correct assuming the following:

- **Creusot, `creusot-std`, Why3 and the provers**, including `creusot-std`'s specifications of `core` and `alloc`.
- **`src/verification/specs/`**, which states without proof what these external functions do:
  - hashbrown's `HashMap` and `HashSet`, including the entry API and indexing;
  - indexmap's `IndexSet`, as a sequence without duplicates;
  - scratchpads' `Scratchpad` and `ScratchpadGuard`, and the `allocator_api2` vectors they return (their sets and maps are hashbrown's);
  - `Option::is_none_or`, `Ordering::then`, `!` on `&bool`, `usize::strict_add`, `Range::is_empty`, `<[T]>::reverse`, `Vec::split_off`, `Vec::append`, `Vec::truncate`, `IndexSet::default` and `IndexSet::from_iter`, which collects the identifiers an iterator yields, in order, without repetitions;
  - `lemma_byte_vec_len`: a `Vec<u8>` holds at most `isize::MAX` bytes, since Rust never allocates more, so merging two of them can't overflow.

  These specifications assume that identifiers' `Hash` and `Eq` implementations agree with their `DeepModel` and terminate, and that the hashers built by a `BuildHasher` agree with each other.
- **`same_id` and `default_hasher`** in `src/lib.rs`, which compare identifiers with `==` and build a hasher with `S::default()` where a function is proven to terminate. They assume that `K`'s `Eq` and `S`'s `Default` implementations terminate, which Creusot can't check for generic types.
- **Strengthened specifications of `IndexSet` and the sorting helpers**: `IndexSet::retain` keeps the remaining elements in order, and `IndexSet::sort_by` and both `sort_by_nodes` helpers sort stably by any comparator which behaves as a total preorder (`compares_by` and `sorted_stably` in `src/verification/mod.rs`). Both follow from indexmap's documentation: `retain` keeps order, and `sort_by` is a stable sort, as is the slice sort the larger branch of `sort_by_nodes` uses.
- **The `#[trusted]` helpers**: `extend_from_set`, `extend_from_set_rev`, `extend_vec_from_set`, `all_entries` and `sort_by_nodes` in `src/dependent/mod.rs`; `count_parents` and `extend_from_set_rev` in `src/independent/graph/mod.rs`; and `all_present`, `contains_other`, `activate_exactly`, `deactivate_all`, `extend_from_hash_set`, `extend_difference`, `extend_from_scratch`, `extend_keys`, `extend_surviving_keys`, `any_active_survivor`, `extend_steps_rev` and `sort_by_nodes` in `src/independent/support.rs`. In `src/wrappers/patchable.rs`, `lists`, `extend_with_parents` and `copied_ids` iterate over a node's parents or children. Their bodies are the iterator and sorting code the methods used to contain inline. hashbrown's and indexmap's iterators, and the arena allocations used for sorting, have no Creusot specifications, and Rust's orphan rule prevents writing iterator specifications for them in this crate, so these helpers' specifications are assumed, including, for those marked `#[check(terminates)]`, that they terminate.
- **`default_contents`** in `src/wrappers/patchable.rs`, which returns `T::default()` and is assumed to return empty contents: `PatchablePathWeave`'s requirement 4, which can't be stated as a precondition since `Default::default` has no specification. The patch operations still check it with an `assert!`.
- **`DiscreteContents` implementations outside of this crate**: they aren't compiled while Creusot is running, so nothing checks that their `len`, `is_empty`, `split` and `merge` meet the trait's contracts. `PatchablePathWeave`'s requirement 3 is a precondition, which a caller must establish for its contents type.

## How the code changed

The control flow and order of operations follow the original code, and the test suites pass unchanged. The differences:

- No method has `#[contract(...)]` annotations anymore, so the `contracts` dependency and its re-export, `universal_weave::contracts`, were removed. Code which used that re-export must depend on the `contracts` crate directly. `creusot-std` was added, without its default features, so `no_std` builds still work.
- `src/contract.rs` only keeps what the archived (rkyv) weaves use. The helpers only the contracts used (`lacks_duplicates`, `valid_path` and `valid_topological_sort`) were removed, and `valid_topology` moved to `src/independent/support.rs`.
- Iterator chains over the weaves' collections became loops over indices, or moved into the trusted helpers above. Reverse iteration became `while i > 0 { i -= 1; ... }`.
- The graph searches (`topological_sort`, `topological_sort_subgraph`, `shortest_path_to_ancestor`, `longest_candidate_path_to_root`, `ancestor_subgraph`, `ancestor_subgraph_reaches`, `descendant_subgraph` and `descendant_subgraph_reaches`) moved from `src/lib.rs` into `src/independent/graph/`, and take `IndependentNode`s instead of any `Node`. Instead of calling a callback for each identifier they find, they push it onto a vector, which the caller then reads. Their archived versions stay in `src/lib.rs`.
- `IndexSet::from_iter([id])` became `IndexSet::with_capacity_and_hasher(1, S::default())` followed by `insert`.
- `split` and `merge_with_parent` relink children by index instead of iterating over the set.
- The `sort_*` methods share `sort_by_nodes`, which contains the code they repeated.
- `validate` moved into free functions taking the weave's fields, which check each node in `validate_node` and use `contains_all`, `all_present` and `all_entries` for their loops. `DependentWeave`'s serde `Deserialize` implementation calls it before building the weave instead of after.
- The weaves' and nodes' `PartialEq` and `Eq` implementations and derived `Default`, `Debug` and `Clone` implementations, and the derived comparison traits of `DiscreteContentResult`, `LayoutItem` and `Step`, aren't compiled while Creusot is running, since Creusot can't translate them for these generic types. They are unchanged otherwise.
- In the functions proven to terminate, closures passed to `Option::is_none_or`, `is_some_and`, `map` and `and_then` became `match`es or `let` chains, `==` on identifiers became `same_id`, `S::default()` became `default_hasher()`, and `for` loops became `while` loops with variants, since Creusot can't show that a closure or a generic `Eq` or `Default` implementation terminates. The getters' and `get_contents_mut`'s `Option::map` also became `match`es, which lets `get_contents_mut` state what its callback returns.
- `ActivePathWeave::set_active_path` takes `impl VerifiableIterator<Item = K>`. Outside of Creusot, every iterator implements `VerifiableIterator`, so callers are unaffected, but implementations of `ActivePathWeave` outside of this crate must update their signature. While Creusot is running, it requires the iterator to have an `IteratorSpec`.
- `DiscreteContents` gained the logic methods `splits` and `merges`, and the type `Unit` with the logic method `units`, which only exist while Creusot is running, so implementations are unaffected. `len` and `is_empty` are specified in terms of `units`.

`DependentWeave`:

- `topological_sort` and `detect_cycles` take `DependentNode`s instead of any `Node`, and `detect_cycles` takes the roots as an `IndexSet`, which `legacy_dependent` now passes too.

`IndependentWeave`:

- `update_node_activity_in_place` and `fix_orphaned_activations` became associated functions taking the weave's fields, so that `insert`, `move_to` and `set_active` can call them while the weave is being changed.
- Code which several methods repeated moved into helpers: `activate_exactly` (making exactly a set of nodes active), `best_scored` (choosing the best-scored parent or child when computing the active path), `extends_active_end`, `any_active`, `first_root_index`, `contains_other`, `all_present_in_slice`, `same_ids` and `index_set_from_slice`, which inserts the identifiers of a slice one by one instead of collecting them.
- `set_active_path` deactivates the active nodes with `deactivate_all`, and then activates each node its iterator yields, calling `next` itself, instead of filtering and extending.
- `longest_candidate_path_to_root` takes the set of candidates, and a set they must also be in, instead of a closure.
- `merge_with_parent` finds the position of the merged node's bookmark before replacing it, in a separate `let`.
- `insert` and `move_to` name the scratch collections they pass to `ancestor_subgraph_reaches` and `descendant_subgraph_reaches`, so proofs can refer to them. `get_path_from` drops its scratchpad guard in an inner block, and `remove`, `remove_tracked`, `move_to` and `set_active_dependent_semantics` destructure the weave (`let Self { nodes, roots, active, .. } = self;`) before borrowing its scratchpad or making their last changes, so that proofs can refer to its fields meanwhile.
- `insert` checks whether a parent is the end of the active path with `extends_active_end`, as `update_node_activity_in_place` does, which reads the active set rather than the parent's `active` flag. The two agree in a valid weave.
- `get_ordered_identifiers_from` and `get_active_path` build their output in a scratch vector before copying it into `output`.
- `fix_orphaned_activations` copies the active identifiers into a scratch vector before searching from each of them, and keeps the roots `ancestor_subgraph` finds, which it previously discarded.
- `valid_topology` skips visited roots with `if` instead of `continue`, and pushes children with `extend_steps_rev`.

`PatchablePathWeave`:

- It moved from `src/wrappers.rs` into `src/wrappers/patchable.rs`, and `src/wrappers.rs` became `src/wrappers/mod.rs`, as the `mod_module_files` lint requires. While Creusot is running, the `wrappers` module (declared inline in `src/lib.rs`) only contains `PatchablePathWeave`.
- The methods which read or patch the active path are written once, in the `path_methods!` macro, and implemented by it for every weave, or only for `IndependentWeave` while Creusot is running, since Creusot verifies a generic method once, through the traits' contracts, which don't describe what the weave does. rustfmt doesn't format the macro's body.
- `for (index, id) in self.scratchpad.iter().enumerate()` became `for index in 0..self.scratchpad.len()` with `let id = &self.scratchpad[index];`, since `enumerate()` requires the vector to hold fewer than `usize::MAX` elements, which Creusot can't show. `insert_at` counts the empty nodes at the start of the active path with a `while` loop instead of `take_while(..).count()`, and `split_at` reverses the active path and iterates over it instead of over `drain(..).rev()`, which has no specification.
- Iterating over a node's parents or children moved into `lists`, `extend_with_parents` and `copied_ids`, and `T::default()` into `default_contents`. `replace`'s `insert_tail_node` closure became an associated function, which takes `prefix_all` and `generate_id`.
- `split_out` and `replace` share two steps, which moved into associated functions: `locate_range`, which scans the active path for where the range starts and ends, and returns `None` where they returned early, and `split_bounds`, which splits the nodes the range starts and ends inside of, in the same order as before (`start_split.map(..)` became an `if let`). `locate_range` gives the node the range starts inside of by its index in the scratchpad. They also set the active path through `set_active_line`, which calls `set_active_path`. Proving each step once keeps the methods' proofs small: the provers would otherwise consider every combination of the steps' branches with each of the methods' own.
- The iterators passed to `set_active_path`, the parents and children of the nodes `insert_at`'s and `insert_tail_node`'s inserts build, and the identifiers `split_bounds` and `insert_tail_node` generate are bound to local variables first, in the same order, so that proofs can refer to them. `locate_range` annotates the types of `start_split` and `end`.
- The derived `Debug` and `Clone` implementations and the `PartialEq` and `Eq` implementations aren't compiled while Creusot is running.

## Not covered

- `ArchivedDependentWeave` and `ArchivedIndependentWeave` (rkyv), including their separate `validate`, `archived_detect_cycles` and `archived_valid_topology`.
- `DependentLoroWeave` and the `legacy` module.
- The conversions between the two weaves (`From` and `TryFrom`), and the `PartialEq`, `Default`, `Clone`, `Debug` and serde implementations. `Deserialize` relies on `validate`, which is verified.
- The layouters, and the wrappers other than `PatchablePathWeave`.
- `PatchablePathWeave` over weaves other than `IndependentWeave`, and its `active_content` and `path_content`, which aren't compiled while Creusot is running: the iterator they return drains the scratchpad, which has no specification, through a closure which unwraps, which would need a precondition. They yield the contents of the nodes `active_path_vec` and `path_vec` return, from the root.
- Termination, except as noted above, allocation failure, and panics or nontermination inside callbacks, comparators or `DiscreteContents` methods. The `Weave` documentation already says a panic may leave a weave malformed.
