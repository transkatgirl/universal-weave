pub mod dependent;
pub mod independent;

pub trait Node<T> {
    fn id(&self) -> u128;
    fn from(&self) -> impl Iterator<Item = u128>;
    fn to(&self) -> impl Iterator<Item = u128>;
    fn is_active(&self) -> bool;
    fn is_bookmarked(&self) -> bool;
    fn contents(&self) -> &T;
}

pub trait Weave<N, T>
where
    N: Node<T>,
{
    fn get_node(&self, id: u128) -> Option<&impl Node<T>>;
    fn get_roots(&self) -> impl Iterator<Item = u128>;
    fn get_bookmarks(&self) -> impl Iterator<Item = u128>;
    fn get_active_threads(&self) -> impl Iterator<Item = u128>;
    fn add_node(&mut self, node: impl Node<T>) -> bool;
    fn set_node_active_status(&mut self, id: u128, value: bool) -> bool;
    fn set_node_bookmarked_status(&mut self, id: u128, value: bool) -> bool;
    fn remove_node(&mut self, id: u128) -> Option<impl Node<T>>;
}

pub trait IndependentWeave<N, T>
where
    N: Node<T>,
    T: IndependentContents,
{
    fn replace_node_parents(&mut self, target: u128, parents: &[u128]) -> bool;
}

pub trait DiscreteWeave<N, T>
where
    N: Node<T>,
    T: DiscreteContents,
{
    fn split_node(&mut self, id: u128, at: usize) -> bool;
    fn merge_with_parent(&mut self, id: u128) -> bool;
}

pub trait DuplicatableWeave<N, T>
where
    N: Node<T>,
    T: DuplicatableContents,
{
    fn find_duplicates(&self, id: u128) -> impl Iterator<Item = u128>;
}

pub enum DiscreetContentSplit<T> {
    Success((T, T)),
    Failure(T),
}

pub trait DiscreteContents: Sized {
    fn split(self, at: usize) -> DiscreetContentSplit<Self>;
    fn merge(self, value: Self) -> Self;
}

pub trait DuplicatableContents {
    fn is_duplicate_of(&self, value: &Self) -> bool;
}

pub trait IndependentContents {}
