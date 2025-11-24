use universal_weave::rkyv::rancor::Error;

pub use ulid;
pub use universal_weave;

pub mod treeless;
pub mod v0;
pub mod versioning;

pub const VERSIONED_WEAVE_FILE_EXTENSION: &str = "tapestry";

use crate::versioning::{MixedData, VersionedBytes};

#[non_exhaustive]
pub enum VersionedWeave {
    V0(v0::TapestryWeave),
}

impl VersionedWeave {
    pub fn from_bytes(value: &[u8]) -> Option<Result<Self, Error>> {
        if let Some(versioned) = VersionedBytes::from_bytes(value) {
            match versioned.version {
                0 => Some(
                    v0::TapestryWeave::from_unversioned_bytes(versioned.data.as_ref())
                        .map(Self::V0),
                ),
                _ => None,
            }
        } else {
            None
        }
    }
    pub fn into_latest(self) -> v0::TapestryWeave {
        match self {
            Self::V0(weave) => weave,
        }
    }
    pub fn to_bytes(self) -> Result<Vec<u8>, Error> {
        let (version, bytes) = match self {
            Self::V0(weave) => (0, weave.to_unversioned_bytes()?),
        };

        Ok(VersionedBytes {
            version,
            data: MixedData::Output(bytes),
        }
        .to_bytes())
    }
}

// TODO:
// - Implement v1 format based on IndependentWeave
//   - Implement diff-based tree updates
//   - Implement prefix-based deduplication?
//   - Implement support for editor undo/redo
//   - Implement event-based invalidation support for multi-user weaves

// Useful reference for future v1 format: https://github.com/transkatgirl/Tapestry-Loom/blob/a232fbbb4119a8a9047ca67a8f1b0cfb772c5bb1/weave/src/document/content/mod.rs
