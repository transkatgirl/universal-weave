//! Utilities for versioning serialized binary data.

use rkyv::{rancor::Fallible, ser::Writer};

/// A set of bytes accompanied by file header information.
///
/// Buffers deserialized using [`rkyv`] must be [aligned to 16-byte boundaries](https://rkyv.org/format/alignment.html). [`VersionedBytes`] is capable of preserving 16-byte memory alignment if the backing byte buffer is correctly aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[must_use]
pub struct VersionedBytes<'a> {
    /// The magic bytes at the start of the file indicating the format used.
    pub format_identifier: [u8; 24],
    /// The format version stored within the header.
    pub version: u64,
    /// The data following the header.
    pub data: &'a [u8],
}

impl<'a> VersionedBytes<'a> {
    /// Tries to deserialize a [`VersionedBytes`] struct from a byte array.
    ///
    /// This can fail in the following cases:
    /// - The specified `format_identifier` does not match the first 24 bytes of the byte array
    /// - The byte array is less than 32 bytes long
    #[allow(clippy::missing_panics_doc, reason = "Should never panic")]
    #[must_use]
    #[inline]
    pub fn try_from_bytes(value: &'a [u8], format_identifier: [u8; 24]) -> Option<Self> {
        if value.starts_with(&format_identifier) && value.len() >= 32 {
            let (version_bytes, data) = value[24..].split_at(8);

            Some(Self {
                format_identifier,
                version: u64::from_le_bytes(version_bytes.try_into().unwrap()),
                data,
            })
        } else {
            None
        }
    }
    /// The total length in bytes after serialization.
    #[must_use]
    #[inline]
    pub const fn output_length(&self) -> usize {
        32_usize.strict_add(self.data.len())
    }
    /// Serializes the header into the specified writer.
    ///
    /// # Errors
    ///
    /// Returns `Err` if writing to the given writer fails.
    #[inline]
    pub fn write_header<W>(&self, writer: &mut W) -> Result<(), <W as Fallible>::Error>
    where
        W: Writer + Fallible,
    {
        writer.write(&self.format_identifier)?;
        writer.write(&self.version.to_le_bytes())?;

        Ok(())
    }
    /// Serializes the header and contents into the specified writer.
    ///
    /// # Errors
    ///
    /// Returns `Err` if writing to the given writer fails.
    #[inline]
    pub fn write<W>(&self, writer: &mut W) -> Result<(), <W as Fallible>::Error>
    where
        W: Writer + Fallible,
    {
        writer.write(&self.format_identifier)?;
        writer.write(&self.version.to_le_bytes())?;
        writer.write(self.data)?;

        Ok(())
    }
}
