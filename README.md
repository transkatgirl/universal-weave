# Universal Weave

General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

## Rationale

Loom implementations *seem* deceptively simple to implement, to the point where it feels silly to use a library for the underlying data structure.

However, it's incredibly easy to create subtle bugs and oversights in your implementation, especially if you're dealing with untrusted user data or you're interested in more advanced features such as CRDT-based collaboration or DAG-based documents.

This library is the culmination of everything I've learned over the past 1 1/2 years of attempting to build a worthy successor to the original Loom. It seems deceptively simple, but it is the end product of a long process that you probably don't want to repeat yourself.

Please [consider donating](https://github.com/sponsors/transkatgirl) if you consider this crate useful.

(These primitives are designed specifically for user-facing applications. Crates like [ego-tree](https://crates.io/crates/ego-tree), [petgraph](https://crates.io/crates/petgraph), [daggy](https://crates.io/crates/daggy), etc, are better suited for general-purpose use.)

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
		- Robust to untrusted inputs (hash collision resistance is not supported for zero-copy deserialization)
		- Format versioning (requires rkyv)
	- Unbounded depth
	- Convenient traversal methods
	- Stable node ordering
		- Node sorting
	- Tree-based Weave implementation
		- CRDT-based collaborative editing (requires loro & rkyv, *experimental*)
	- DAG-based Weave implementation (*experimental*)
		- Node moving
	- Support for Weave wrapper implementations
		- Built-in action queuing wrapper (can be used to implement undo/redo)
- Supports `no_std` environments (requires `alloc`)
	- Loro must be disabled when building for `no_std`

TODO: Publish to crates.io and add badges to README
