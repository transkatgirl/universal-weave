// TODO: Use a formal verifier (such as Creusot, Kani, Verus, etc...) once one of them supports enough of the language features
pub mod dependent;
/// WIP
pub mod independent;

pub use rkyv;
use rkyv::{Archive, Deserialize, Serialize, rancor::Fallible};

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
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains(&self, id: &u128) -> bool;
    fn get_node(&self, id: &u128) -> Option<&N>;
    fn get_roots(&self) -> impl Iterator<Item = u128>;
    fn get_bookmarks(&self) -> impl Iterator<Item = u128>;
    fn get_active_threads(&self) -> impl Iterator<Item = u128>;
    fn add_node(&mut self, node: N) -> bool;
    fn set_node_active_status(&mut self, id: &u128, value: bool) -> bool;
    fn set_node_bookmarked_status(&mut self, id: &u128, value: bool) -> bool;
    fn remove_node(&mut self, id: &u128) -> Option<N>;
}

pub trait IndependentWeave<N, T>
where
    N: Node<T>,
    T: IndependentContents,
{
    fn replace_node_parents(&mut self, target: &u128, parents: &[u128]) -> bool;
}

pub trait DiscreteWeave<N, T>
where
    N: Node<T>,
    T: DiscreteContents,
{
    fn split_node(&mut self, id: &u128, at: usize, new_id: u128) -> bool;
    fn merge_with_parent(&mut self, id: &u128) -> bool;
}

pub trait DuplicatableWeave<N, T>
where
    N: Node<T>,
    T: DuplicatableContents,
{
    fn find_duplicates(&self, id: &u128) -> impl Iterator<Item = u128>;
}

pub enum DiscreteContentResult<T> {
    Two((T, T)),
    One(T),
}

pub trait DiscreteContents: Sized {
    fn split(self, at: usize) -> DiscreteContentResult<Self>;
    fn merge(self, value: Self) -> DiscreteContentResult<Self>;
}

pub trait DuplicatableContents {
    fn is_duplicate_of(&self, value: &Self) -> bool;
}

pub trait IndependentContents {}

pub enum DowContents<'a, T>
where
    T: Archive,
{
    Archived(&'a T::Archived),
    Owned(T),
}

impl<'a, T> DowContents<'a, T>
where
    T: Archive,
{
    pub fn is_archived(&self) -> bool {
        match &self {
            Self::Archived(_) => true,
            Self::Owned(_) => false,
        }
    }
    pub fn is_owned(&self) -> bool {
        match &self {
            Self::Archived(_) => false,
            Self::Owned(_) => true,
        }
    }
    pub fn to_owned<D>(self, deserializer: &mut D) -> Result<Self, D::Error>
    where
        D: rkyv::rancor::Fallible + ?Sized,
        T::Archived: Deserialize<T, D>,
    {
        match self {
            Self::Archived(archived) => Ok(Self::Owned(archived.deserialize(deserializer)?)),
            Self::Owned(owned) => Ok(Self::Owned(owned)),
        }
    }
}

impl<'a, T> Archive for DowContents<'a, T>
where
    T: Archive,
{
    type Archived = T::Archived;
    type Resolver = T::Resolver;

    fn resolve(
        &self,
        resolver: Self::Resolver,
        out: rkyv::Place<<DowContents<'a, T> as Archive>::Archived>,
    ) {
        match self {
            Self::Archived(archived) => {
                todo!()
            }
            Self::Owned(owned) => {
                owned.resolve(resolver, out);
            }
        }
    }
}

impl<'a, T, S> Serialize<S> for DowContents<'a, T>
where
    T: Archive + Serialize<S>,
    S: Fallible + Sized,
{
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, <S as Fallible>::Error> {
        match self {
            Self::Archived(archived) => {
                todo!()
            }
            Self::Owned(owned) => owned.serialize(serializer),
        }
    }
}
