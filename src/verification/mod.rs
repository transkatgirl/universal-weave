//! Definitions used to verify [`DependentWeave`](crate::dependent::DependentWeave) and
//! [`IndependentWeave`](crate::independent::IndependentWeave) with
//! [Creusot](https://github.com/creusot-rs/creusot).
//!
//! This module only exists while Creusot is verifying the crate. See `VERIFICATION.md` for what
//! is proven and what is trusted.
//!
//! - [`specs`]: trusted specifications of the external functions which the verified code calls.

pub mod specs;

use core::cmp::Ordering;

use creusot_std::{
    logic::{FMap, FSet, Mapping, such_that},
    prelude::*,
};

/// The logical model of a collection from another crate.
///
/// Creusot's [`View`] would be used instead, but Rust's orphan rule prevents implementing it for
/// types from other crates.
pub trait Model {
    /// The type of the model.
    type ModelTy;

    /// The model of `self`.
    #[logic]
    fn model(self) -> Self::ModelTy;
}

/// The identifier model of `K`: identifiers which are equal according to [`Eq`] have the same
/// model.
pub type Id<K> = <K as DeepModel>::DeepModelTy;

/// `s` contains no element twice.
#[logic(open)]
pub fn no_dup<T>(s: Seq<T>) -> bool {
    pearlite! { forall<i, j> 0 <= i && i < j && j < s.len() ==> s[i] != s[j] }
}

/// `a` and `b` contain the same elements, regardless of order and multiplicity.
#[logic(open)]
pub fn same_elements<T>(a: Seq<T>, b: Seq<T>) -> bool {
    pearlite! { forall<x> a.contains(x) == b.contains(x) }
}

/// Appending `x` adds exactly `x`.
#[logic]
#[ensures(s.snoc(x).contains(y) == (s.contains(y) || y == x))]
pub fn lemma_snoc_contains<T>(s: Seq<T>, x: T, y: T) {
    proof_assert!(s.snoc(x)[s.len()] == x);
    proof_assert!(forall<i> 0 <= i && i < s.len() ==> s.snoc(x)[i] == s[i]);
}

/// A concatenation contains exactly the elements of its parts.
#[logic]
#[ensures(a.concat(b).contains(y) == (a.contains(y) || b.contains(y)))]
pub fn lemma_concat_contains<T>(a: Seq<T>, b: Seq<T>, y: T) {
    proof_assert!(forall<i> 0 <= i && i < a.len() ==> a.concat(b)[i] == a[i]);
    proof_assert!(forall<i> 0 <= i && i < b.len() ==> a.concat(b)[a.len() + i] == b[i]);
}

/// Appending an element preserves the absence of duplicates exactly when the element is new.
#[logic]
#[ensures(no_dup(s.snoc(x)) == (no_dup(s) && !s.contains(x)))]
pub fn lemma_no_dup_snoc<T>(s: Seq<T>, x: T) {
    proof_assert!(s.snoc(x)[s.len()] == x);
    proof_assert!(forall<i> 0 <= i && i < s.len() ==> s.snoc(x)[i] == s[i]);
}

/// Concatenating disjoint sequences without duplicates gives a sequence without duplicates.
#[logic]
#[requires(no_dup(a) && no_dup(b))]
#[requires(forall<x> a.contains(x) ==> !b.contains(x))]
#[ensures(no_dup(a.concat(b)))]
pub fn lemma_no_dup_concat<T>(a: Seq<T>, b: Seq<T>) {
    proof_assert!(forall<i> 0 <= i && i < a.len() ==> a.concat(b)[i] == a[i]);
    proof_assert!(forall<i> 0 <= i && i < b.len() ==> a.concat(b)[a.len() + i] == b[i]);
    proof_assert!(forall<i> 0 <= i && i < a.len() ==> a.contains(a[i]));
    proof_assert!(forall<i> 0 <= i && i < b.len() ==> b.contains(b[i]));
}

/// Membership in `set`, as a mapping.
///
/// Passing this instead of a closure lets separate mentions of the same set's membership be
/// recognized as equal: closures are compared by their definitions, which provers can't do.
#[logic]
#[ensures(forall<k: I> result[k] == set.contains(&k))]
pub fn member_of<I>(set: FSet<I>) -> Mapping<I, bool> {
    pearlite! { |k: I| set.contains(&k) }
}

/// Maps an identifier to its model.
#[logic(open)]
pub fn id_of<K: DeepModel>() -> Mapping<K, Id<K>> {
    |k: K| k.deep_model()
}

/// The models of the identifiers in `s`.
#[logic(open)]
pub fn ids<K: DeepModel>(s: Seq<K>) -> Seq<Id<K>> {
    s.map(id_of())
}

/// Pushing an identifier pushes its model.
#[logic]
#[ensures(ids(s.snoc(x)) == ids(s).snoc(x.deep_model()))]
pub fn lemma_ids_snoc<K: DeepModel>(s: Seq<K>, x: K) {
    s.snoc_map(x, id_of());
}

/// `(a ++ b).snoc(x) == a ++ b.snoc(x)`
#[logic]
#[ensures(a.concat(b).snoc(x) == a.concat(b.snoc(x)))]
pub fn lemma_concat_snoc<T>(a: Seq<T>, b: Seq<T>, x: T) {
    proof_assert!(a.concat(b).snoc(x).ext_eq(a.concat(b.snoc(x))));
}

/// The first element of `s`, if any.
#[logic(open)]
pub fn first<T>(s: Seq<T>) -> Option<T> {
    if s.len() == 0 { None } else { Some(s[0]) }
}

/// Reversing a sequence keeps its elements.
#[logic]
#[ensures(s.reverse().len() == s.len())]
#[ensures(forall<x> s.reverse().contains(x) == s.contains(x))]
#[ensures(no_dup(s) ==> no_dup(s.reverse()))]
pub fn lemma_reverse<T>(s: Seq<T>) {
    proof_assert!(forall<i> 0 <= i && i < s.len() ==> s.reverse()[i] == s[s.len() - 1 - i]);
}

/// The prefixes of the reverse of `s` grow by the elements of `s` from its end: taking one more
/// element of it takes `s[i]`, if the ones taken so far are those after `i`.
#[logic]
#[ensures(result == (0 <= i && i < s.len()))]
#[ensures(result ==> s.reverse().subsequence(0, s.len() - i)
    == s.reverse().subsequence(0, s.len() - 1 - i).snoc(s[i]))]
pub fn reverse_prefix_step_ok<T>(s: Seq<T>, i: Int) -> bool {
    pearlite! {
        let pre = 0 <= i && i < s.len();
        proof_assert!(pre ==> s.reverse()[s.len() - 1 - i] == s[i]);
        proof_assert!(pre ==> s.reverse().subsequence(0, s.len() - i)
            .ext_eq(s.reverse().subsequence(0, s.len() - 1 - i).snoc(s[i])));
        pre
    }
}

/// Pushing an identifier onto an empty sequence gives the singleton of its model.
#[logic]
#[requires(s.len() == 0)]
#[ensures(ids(s.snoc(x)) == Seq::singleton(x.deep_model()))]
pub fn lemma_ids_singleton<K: DeepModel>(s: Seq<K>, x: K) {
    proof_assert!(ids(s.snoc(x)).ext_eq(Seq::singleton(x.deep_model())));
}

/// Prefixes of a sequence grow one element at a time.
#[logic]
#[requires(0 <= i && i < s.len())]
#[ensures(s.subsequence(0, i + 1).contains(x) == (s.subsequence(0, i).contains(x) || x == s[i]))]
pub fn lemma_prefix_contains<T>(s: Seq<T>, i: Int, x: T) {
    proof_assert!(forall<j> 0 <= j && j < i ==> s.subsequence(0, i + 1)[j] == s.subsequence(0, i)[j]);
}

/// The whole sequence is a prefix of itself.
#[logic]
#[ensures(s.subsequence(0, s.len()) == s)]
pub fn lemma_full_prefix<T>(s: Seq<T>) {
    proof_assert!(s.subsequence(0, s.len()).ext_eq(s));
}

/// Pushing onto the empty sequence gives a singleton.
#[logic]
#[ensures(Seq::empty().snoc(x) == Seq::singleton(x))]
pub fn lemma_empty_snoc<T>(x: T) {
    proof_assert!(Seq::empty().snoc(x).ext_eq(Seq::singleton(x)));
}

/// A sequence without duplicates is no longer than any sequence containing its elements.
#[logic]
#[requires(no_dup(a))]
#[requires(forall<x> a.contains(x) ==> b.contains(x))]
#[ensures(a.len() <= b.len())]
#[variant(a.len())]
pub fn lemma_subset_len<T>(a: Seq<T>, b: Seq<T>) {
    if a.len() > 0 {
        let x = a[a.len() - 1];
        let t = a.subsequence(0, a.len() - 1);
        pearlite! {
            let j = such_that(|j: Int| 0 <= j && j < b.len() && b[j] == x);
            let c = b.subsequence(0, j).concat(b.subsequence(j + 1, b.len()));
            proof_assert!(forall<i> 0 <= i && i < j ==> c[i] == b[i]);
            proof_assert!(forall<i> j < i && i < b.len() ==> c[i - 1] == b[i]);
            proof_assert!(c.len() == b.len() - 1);
            proof_assert!(forall<i> 0 <= i && i < j ==> 0 <= i && i < c.len() && c.contains(b[i]));
            proof_assert!(forall<i> j < i && i < b.len() ==> 0 <= i - 1 && i - 1 < c.len() && c.contains(b[i]));
            proof_assert!(forall<y> t.contains(y) ==> y != x && b.contains(y));
            proof_assert!(forall<y> b.contains(y) && y != x ==> exists<i> 0 <= i && i < b.len() && i != j && b[i] == y);
            proof_assert!(forall<y> t.contains(y) ==> c.contains(y));
            lemma_subset_len(t, c)
        }
    }
}

/// The models of no identifiers are no models.
#[logic]
#[requires(s.len() == 0)]
#[ensures(ids(s) == Seq::empty())]
#[ensures(forall<t: Seq<Id<K>>> ids(s).concat(t) == t)]
pub fn lemma_ids_empty<K: DeepModel>(s: Seq<K>) {
    proof_assert!(ids(s).ext_eq(Seq::empty()));
    proof_assert!(forall<t: Seq<Id<K>>> Seq::empty().concat(t).ext_eq(t));
}

/// Maps with the same keys have the same length.
#[logic]
#[requires(forall<k> a.contains(&k) == b.contains(&k))]
#[ensures(a.len() == b.len())]
pub fn lemma_same_keys_len<K, V, W>(a: FMap<K, V>, b: FMap<K, W>) {
    proof_assert!(a.keys().ext_eq(b.keys()));
}

/// An upper bound on the values of `f` over the elements of `s`, which is at least zero.
#[logic]
#[ensures(0 <= result)]
#[ensures(forall<i> 0 <= i && i < s.len() ==> f[s[i]] < result)]
#[variant(s.len())]
pub fn bound<T>(s: Seq<T>, f: Mapping<T, Int>) -> Int {
    if s.len() == 0 {
        0
    } else {
        let rest = bound(s.subsequence(0, s.len() - 1), f);
        let last = f[s[s.len() - 1]] + 1;
        if rest < last { last } else { rest }
    }
}

/// The models of reversed identifiers are the reversed models.
#[logic]
#[ensures(ids(s.reverse()) == ids(s).reverse())]
pub fn lemma_ids_reverse<K: DeepModel>(s: Seq<K>) {
    proof_assert!(ids(s.reverse()).ext_eq(ids(s).reverse()));
}

/// The models of a prefix of identifiers are the prefix of the models.
#[logic]
#[requires(0 <= n && n <= s.len())]
#[ensures(ids(s.subsequence(0, n)) == ids(s).subsequence(0, n))]
pub fn lemma_ids_prefix<K: DeepModel>(s: Seq<K>, n: Int) {
    proof_assert!(ids(s.subsequence(0, n)).ext_eq(ids(s).subsequence(0, n)));
}

/// An upper bound on the values of `f` over the elements of `set`.
#[logic]
#[ensures(forall<x> set.contains(&x) ==> f[x] < result)]
#[variant(set.len())]
pub fn set_bound<T>(set: FSet<T>, f: Mapping<T, Int>) -> Int {
    if set.is_empty() {
        0
    } else {
        let x = set.peek();
        let rest = set_bound(set.remove(x), f);
        if rest < f[x] + 1 { f[x] + 1 } else { rest }
    }
}

/// `f` increases from one element of `s` to the next.
#[logic(open)]
pub fn increasing_steps<T>(s: Seq<T>, f: Mapping<T, Int>) -> bool {
    pearlite! { forall<i> 0 <= i && i + 1 < s.len() ==> f[s[i]] < f[s[i + 1]] }
}

/// If `f` increases between consecutive elements of `s`, it increases along all of `s`.
#[logic]
#[requires(increasing_steps(s, f))]
#[requires(0 <= a && a < b && b < s.len())]
#[ensures(f[s[a]] < f[s[b]])]
#[variant(b - a)]
pub fn lemma_increasing_at<T>(s: Seq<T>, f: Mapping<T, Int>, a: Int, b: Int) {
    if b > a + 1 {
        lemma_increasing_at(s, f, a + 1, b)
    }
}

/// If `f` increases between consecutive elements of `s`, it increases along all of `s`.
#[logic]
#[requires(increasing_steps(s, f))]
#[ensures(forall<a, b> 0 <= a && a < b && b < s.len() ==> f[s[a]] < f[s[b]])]
pub fn lemma_increasing<T>(s: Seq<T>, f: Mapping<T, Int>) {
    let _ = lemma_increasing_at::<T>;
}

/// A map whose keys are all keys of another map is no longer.
#[logic]
#[requires(forall<k> a.contains(&k) ==> b.contains(&k))]
#[ensures(a.len() <= b.len())]
pub fn lemma_keys_subset_len<K, V, W>(a: FMap<K, V>, b: FMap<K, W>) {
    proof_assert!(a.keys().is_subset(b.keys()));
}

/// A set of keys of `m` is no larger than `m`.
#[logic]
#[requires(forall<k> a.contains(&k) ==> m.contains(&k))]
#[ensures(a.len() <= m.len())]
pub fn lemma_set_in_keys_len<K, V>(a: FSet<K>, m: FMap<K, V>) {
    proof_assert!(a.is_subset(m.keys()));
}

/// A subset is no larger.
#[logic]
#[requires(forall<k> a.contains(&k) ==> b.contains(&k))]
#[ensures(a.len() <= b.len())]
pub fn lemma_fset_subset_len<T>(a: FSet<T>, b: FSet<T>) {
    proof_assert!(a.is_subset(b));
}

/// The elements of `new` appear in `old` in the same order. A sequence without duplicates is
/// determined by its elements and by this order.
#[logic(open)]
pub fn in_order<T>(new: Seq<T>, old: Seq<T>) -> bool {
    pearlite! {
        forall<i, j> 0 <= i && i < j && j < new.len()
            ==> exists<k, l> 0 <= k && k < l && l < old.len() && old[k] == new[i] && old[l] == new[j]
    }
}

/// A sequence is in its own order.
#[logic]
#[ensures(in_order(s, s))]
pub fn lemma_in_order_refl<T>(s: Seq<T>) {}

/// Removing an element keeps the others in order.
#[logic]
#[requires(0 <= i && i < s.len())]
#[ensures(in_order(s.removed(i), s))]
pub fn lemma_in_order_removed<T>(s: Seq<T>, i: Int) {
    proof_assert!(forall<j> 0 <= j && j < i ==> s.removed(i)[j] == s[j]);
    proof_assert!(forall<j> i <= j && j + 1 < s.len() ==> s.removed(i)[j] == s[j + 1]);
}

/// Keeping the order is transitive.
#[logic]
#[requires(in_order(a, b))]
#[requires(in_order(b, c))]
#[ensures(in_order(a, c))]
pub fn lemma_in_order_trans<T>(a: Seq<T>, b: Seq<T>, c: Seq<T>) {}

/// `le` is a total preorder: it orders any two values, transitively.
#[logic(open)]
pub fn total_preorder<T>(le: Mapping<T, Mapping<T, bool>>) -> bool {
    pearlite! {
        (forall<a, b> le[a][b] || le[b][a])
            && forall<a, b, c> le[a][b] && le[b][c] ==> le[a][c]
    }
}

/// How `le` orders `a` and `b`.
#[logic(open)]
pub fn ordering_by<T>(le: Mapping<T, Mapping<T, bool>>, a: T, b: T) -> Ordering {
    pearlite! {
        if le[a][b] {
            if le[b][a] { Ordering::Equal } else { Ordering::Less }
        } else {
            Ordering::Greater
        }
    }
}

/// Every call of `cmp`, in any state it can reach, compares its arguments as `le` orders their
/// images under `key`.
#[logic(open, prophetic)]
pub fn compares_by<A, B, F>(cmp: F, key: Mapping<A, B>, le: Mapping<B, Mapping<B, bool>>) -> bool
where
    F: FnMut(&A, &A) -> Ordering,
{
    pearlite! {
        forall<f: F, g: F, a: &A, b: &A, o: Ordering> cmp.hist_inv(f) && f.postcondition_mut((a, b), g, o)
            ==> o == ordering_by(le, key[*a], key[*b])
    }
}

/// The order which `le` induces through `key`.
#[logic(open)]
pub fn order_by<T, U>(
    le: Mapping<U, Mapping<U, bool>>,
    key: Mapping<T, U>,
) -> Mapping<T, Mapping<T, bool>> {
    |a| |b| le[key[a]][key[b]]
}

/// The identity mapping.
#[logic(open)]
pub fn identity<T>() -> Mapping<T, T> {
    |x| x
}

/// `new` is `old` sorted stably by `le`: a permutation of it in which every element is at most the
/// elements after it, and equivalent elements keep their order.
#[logic(open)]
pub fn sorted_stably<T>(new: Seq<T>, old: Seq<T>, le: Mapping<T, Mapping<T, bool>>) -> bool {
    pearlite! {
        new.permutation_of(old)
            && (forall<i, j> 0 <= i && i < j && j < new.len() ==> le[new[i]][new[j]])
            && forall<i, j> 0 <= i && i < j && j < new.len() && le[new[j]][new[i]]
                ==> exists<k, l> 0 <= k && k < l && l < old.len() && old[k] == new[i] && old[l] == new[j]
    }
}

/// The value `map` associates with each key.
#[logic(open)]
pub fn value_of<K, V>(map: FMap<K, V>) -> Mapping<K, V> {
    pearlite! { |k| map[k] }
}

/// `IndexSet::shift_remove` keeps the other elements in order.
#[logic]
#[requires(forall<i> 0 <= i && i < old.len() && old[i] == x ==> new == old.removed(i))]
#[requires(!old.contains(x) ==> new == old)]
#[ensures(in_order(new, old))]
pub fn lemma_in_order_shift_remove<T>(new: Seq<T>, old: Seq<T>, x: T) {
    if old.contains(x) {
        let i = such_that(|i| 0 <= i && i < old.len() && old[i] == x);
        lemma_in_order_removed(old, i)
    } else {
        lemma_in_order_refl(old)
    }
}

/// `new` is `old` without `x`, as `IndexSet::shift_remove` leaves it: the other elements keep
/// their order.
#[logic(open)]
pub fn shift_removed<T>(new: Seq<T>, old: Seq<T>, x: T) -> bool {
    pearlite! {
        (forall<i> 0 <= i && i < old.len() && old[i] == x ==> new == old.removed(i))
            && (!old.contains(x) ==> new == old)
    }
}

/// `new` is `old` with `a` replaced by `b` at the same position, as `IndexSet::replace_index`
/// leaves it.
#[logic(open)]
pub fn replaced_in_place<T>(new: Seq<T>, old: Seq<T>, a: T, b: T) -> bool {
    pearlite! { forall<i> 0 <= i && i < old.len() && old[i] == a ==> new == old.set(i, b) }
}

/// Setting the element of `old` at the position of `a` to `b` replaces `a` in place.
#[logic]
#[requires(no_dup(old))]
#[requires(0 <= i && i < old.len() && old[i] == a)]
#[requires(new == old.set(i, b))]
#[ensures(replaced_in_place(new, old, a, b))]
pub fn lemma_replaced_in_place<T>(new: Seq<T>, old: Seq<T>, i: Int, a: T, b: T) {
    proof_assert!(forall<j> 0 <= j && j < old.len() && old[j] == a ==> j == i)
}

/// `new` is the bookmarks `old` once the bookmark of `id` is set to `value`: a new bookmark comes
/// last, and removing one keeps the others in order.
#[logic(open)]
pub fn bookmark_set<T>(new: Seq<T>, old: Seq<T>, id: T, value: bool) -> bool {
    pearlite! {
        if value {
            new == if old.contains(id) { old } else { old.snoc(id) }
        } else {
            shift_removed(new, old, id)
        }
    }
}

/// `states` are the states of a callback, from its first, as it is called with each of `args` in
/// turn.
#[logic(open, prophetic)]
pub fn calls<A, F: FnMut(A)>(states: Seq<F>, args: Seq<A>) -> bool {
    pearlite! {
        states.len() == args.len() + 1
            && forall<i> 0 <= i && i < args.len()
                ==> states[i].postcondition_mut((args[i],), states[i + 1], ())
    }
}

/// Calling the callback once more adds its new state.
#[logic(prophetic)]
#[requires(calls(states, args))]
#[requires(states[args.len()].postcondition_mut((x,), f, ()))]
#[ensures(calls(states.snoc(f), args.snoc(x)))]
pub fn lemma_calls_snoc<A, F: FnMut(A)>(states: Seq<F>, args: Seq<A>, x: A, f: F) {
    proof_assert!(forall<i> 0 <= i && i < states.len() ==> states.snoc(f)[i] == states[i]);
    proof_assert!(forall<i> 0 <= i && i < args.len() ==> args.snoc(x)[i] == args[i]);
}

/// The callback `f` is called with each of `args` in turn, and then dropped.
#[logic(open, prophetic)]
pub fn fed<A, F: FnMut(A)>(f: F, args: Seq<A>) -> bool {
    pearlite! {
        exists<states: Seq<F>> calls(states, args) && states[0] == f && resolve(states[args.len()])
    }
}

/// Iterating over `iter` until it ends produces `items`.
#[logic(open, prophetic)]
pub fn iterates<I: IteratorSpec>(iter: I, items: Seq<I::Item>) -> bool {
    pearlite! { exists<done: &mut I> iter.produces(items, *done) && done.completed() }
}

/// `s` without repetitions: the first occurrence of each element, in order.
#[logic(open)]
#[variant(s.len())]
pub fn dedup<T>(s: Seq<T>) -> Seq<T> {
    pearlite! {
        if s.len() == 0 {
            Seq::empty()
        } else {
            let rest = dedup(s.subsequence(0, s.len() - 1));
            if rest.contains(s[s.len() - 1]) { rest } else { rest.snoc(s[s.len() - 1]) }
        }
    }
}

/// Removing repetitions keeps every element, and leaves no duplicates.
#[logic]
#[ensures(forall<x> dedup(s).contains(x) == s.contains(x))]
#[ensures(no_dup(dedup(s)))]
#[variant(s.len())]
pub fn lemma_dedup<T>(s: Seq<T>) {
    if s.len() > 0 {
        let t = s.subsequence(0, s.len() - 1);
        let x = s[s.len() - 1];
        lemma_dedup(t);
        lemma_no_dup_snoc(dedup(t), x);
        let _ = lemma_snoc_contains::<T>;
        pearlite! {
            proof_assert!(forall<i> 0 <= i && i < t.len() ==> t[i] == s[i]);
            proof_assert!(forall<y> s.contains(y) == (t.contains(y) || y == x))
        }
    }
}

/// A sequence without duplicates has no repetitions to remove.
#[logic]
#[requires(no_dup(s))]
#[ensures(dedup(s) == s)]
#[variant(s.len())]
pub fn lemma_dedup_no_dup<T>(s: Seq<T>) {
    if s.len() > 0 {
        let t = s.subsequence(0, s.len() - 1);
        pearlite! {
            proof_assert!(forall<i> 0 <= i && i < t.len() ==> t[i] == s[i]);
            proof_assert!(no_dup(t) && !t.contains(s[s.len() - 1]))
        };
        lemma_dedup_no_dup(t);
        proof_assert!(t.snoc(s[s.len() - 1]).ext_eq(s))
    }
}

/// Removing repetitions from one more element of `s` adds that element if it is new.
#[logic]
#[requires(0 <= i && i < s.len())]
#[ensures(dedup(s.subsequence(0, i + 1)) == if dedup(s.subsequence(0, i)).contains(s[i]) {
    dedup(s.subsequence(0, i))
} else {
    dedup(s.subsequence(0, i)).snoc(s[i])
})]
pub fn lemma_dedup_prefix<T>(s: Seq<T>, i: Int) {
    proof_assert!(
        s.subsequence(0, i + 1)
            .subsequence(0, i)
            .ext_eq(s.subsequence(0, i))
    )
}

/// Sorting by two orders which agree on the sorted elements gives the same results.
#[logic]
#[requires(sorted_stably(new, old, a))]
#[requires(same_elements(new, old))]
#[requires(forall<x, y> old.contains(x) && old.contains(y) ==> a[x][y] == b[x][y])]
#[ensures(sorted_stably(new, old, b))]
pub fn lemma_sorted_agree<T>(
    new: Seq<T>,
    old: Seq<T>,
    a: Mapping<T, Mapping<T, bool>>,
    b: Mapping<T, Mapping<T, bool>>,
) {
    proof_assert!(forall<i> 0 <= i && i < new.len() ==> new.contains(new[i]) && old.contains(new[i]));
}
