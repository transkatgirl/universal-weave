# Universal Weave

A Rust library providing <!-- well-tested  -->general-purpose building blocks for [Loom](https://generative.ink/posts/loom-interface-to-the-multiverse/) implementations.

Please [consider donating](https://github.com/sponsors/transkatgirl) if you find this crate useful.

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
		- Zero-copy deserialization (rkyv only)
		- Format versioning (rkyv only)
	- Unbounded depth (unsupported for WASM and [some niche targets](https://github.com/rust-lang/stacker/#platform-support))
	- Convenient traversal methods
	- Stable node ordering
		- Node sorting
	- Tree-based Weave implementation
		- CRDT-based collaborative editing (uses loro & rkyv, *experimental*)
	- DAG-based Weave implementation (**experimental WIP; not recommended for use**)
		- Node moving
	- General-purpose weave wrappers: Action queuing (can be used to implement undo/redo)
<!--
- Reliability:
	- Built using design-by-contract principles
		- Makes heavy use of debug assertions
		- Offers interfaces for applying internal correctness assertions at runtime
			- It's recommended that you apply these assertions when persistently storing document state
	- Heavily property tested
	- Entirely human written codebase
-->

(While this library aims to have *reasonably decent* performance and a *reasonably flexible* API, these primitives are designed specifically for user-facing applications. Crates like [ego-tree](https://crates.io/crates/ego-tree), [petgraph](https://crates.io/crates/petgraph), [daggy](https://crates.io/crates/daggy), etc, are better suited for general-purpose use.)

This library is a Rust crate which should be included in your project through the use of git submodules. In the future, once the API has been stabilized and the code has been throughly tested, this crate will be published to crates.io.
