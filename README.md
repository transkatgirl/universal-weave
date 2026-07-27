# Universal Weave

General-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

This library makes building Loom implementations easier by providing flexible and reliable abstractions over the underlying algorithms.

Please [consider donating](https://github.com/sponsors/transkatgirl) if you consider this crate useful.

Features:
- Nodes:
	- Activation/deactivation
	- Bookmarking
	- Editing
	- Splitting
	- Merging
	- Deduplication
- Weaves:
	- Serialization and deserialization (supports rkyv, serde, wincode)
		- Zero-copy deserialization (requires rkyv)
		- Format versioning (requires rkyv)
	- Unbounded depth (unsupported for WASM and [some niche targets](https://github.com/rust-lang/stacker/#platform-support))
	- Convenient traversal methods
	- Stable node ordering
		- Node sorting
	- Tree-based Weave implementation
		- CRDT-based collaborative editing (requires loro & rkyv, *experimental*)
	- DAG-based Weave implementation (*experimental*)
		- Node moving
	- General-purpose weave wrappers: Action queuing (can be used to implement undo/redo)
<!--
- Reliability:
	- Built using design-by-contract principles
		- Makes heavy use of debug assertions
		- Offers interfaces for applying correctness assertions at runtime
	- Heavily property tested
	- Heavily linted
-->

(While this library aims to have *reasonably decent* performance and a *reasonably flexible* API, these primitives are designed specifically for user-facing applications. Crates like [ego-tree](https://crates.io/crates/ego-tree), [petgraph](https://crates.io/crates/petgraph), [daggy](https://crates.io/crates/daggy), etc, are better suited for general-purpose use.)

TODO: Complete 0.1.0 checklist

TODO: Publish to crates.io and add link to README
