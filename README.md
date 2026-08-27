# Universal Weave

General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

[![crates.io](https://img.shields.io/crates/v/universal-weave.svg)](https://crates.io/crates/universal-weave) [![docs.rs](https://img.shields.io/docsrs/universal-weave.svg)](https://docs.rs/universal-weave) [![Unlicense license](https://img.shields.io/crates/l/universal-weave)](https://github.com/transkatgirl/universal-weave/blob/main/LICENSE)

## Rationale

Loom implementations are deceptively simple to prototype.

However, as the feature set grows, they become incredibly difficult to get right. Features like CRDT-based collaboration or DAG-based documents open up whole new classes of subtle bugs that can slowly erode user trust.

You *really* shouldn't reinvent the wheel if you don't have to.

This library is the culmination of everything I've learned over the past 1 1/2 years of attempting to build a worthy successor to [the original Loom](https://github.com/socketteer/loom). It's highly flexible, empowering users to more easily build branching interfaces for literally anything.

Please [consider donating](https://github.com/sponsors/transkatgirl) if you consider this crate useful.

(These primitives are designed specifically for user-facing applications. Crates like [ego-tree](https://crates.io/crates/ego-tree), [petgraph](https://crates.io/crates/petgraph), [daggy](https://crates.io/crates/daggy), etc, are better suited for general-purpose or extreme use cases.)

## Features

- Nodes:
	- Activation/deactivation
	- Bookmarking
	- Editing
	- Splitting
	- Merging
	- Deduplication
- Weaves:
	- Serialization and deserialization (supports rkyv and serde)
		- Zero-copy deserialization (requires rkyv)
		- Robust to untrusted inputs
			- Hash collision resistance is not supported with rkyv
		- Format versioning (requires rkyv)
	- Unbounded depth
	- Convenient traversal methods
	- Stable node ordering
		- Node sorting
	- Tree-based Weave implementation
		- CRDT-based collaborative editing (requires loro & rkyv, *experimental*)
	- DAG-based Weave implementation
		- Node moving
	- Support for Weave wrapper implementations
		- Built-in action queuing wrapper (can be used to implement undo/redo)
- Supports `no_std` environments (requires `alloc`)
	- Loro must be disabled when building for `no_std`
	- Recommended heap size is ~1.6 MB per 1,000 nodes for IndependentWeave + IndependentLayouter
		- Significant memory efficiency improvements are planned for future releases
