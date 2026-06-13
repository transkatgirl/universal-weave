//! Utilities for versioning serialized binary data

use std::io::{self, Write};

/// A set of bytes accompanied by file header information
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct VersionedBytes<'a> {
    /// The magic bytes at the start of the file indicating the format used
    pub format_identifier: [u8; 24],
    /// The format version stored within the header
    pub version: u64,
    /// The data following the header
    pub data: &'a [u8],
}

impl<'a> VersionedBytes<'a> {
    /// Tries to deserialize a [`VersionedBytes`] struct from a byte array
    ///
    /// This can fail in the following cases:
    /// - The specified `format_identifier` does not match the first 24 bytes of the byte array
    /// - The byte array is less than 32 bytes long
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
    /// The total length in bytes after serialization
    pub fn output_length(&self) -> usize {
        32 + self.data.len()
    }
    /// Returns the serialized header bytes
    pub fn header_bytes(&self) -> [u8; 32] {
        let mut header: [u8; 32] = [0; 32];
        header[..24].copy_from_slice(&self.format_identifier);
        header[24..].copy_from_slice(&self.version.to_le_bytes());

        header
    }
    /// Serializes the header and contents into the specified writer
    pub fn write_bytes(&self, output: &mut impl Write) -> Result<(), io::Error> {
        output.write_all(&self.format_identifier)?;
        output.write_all(&self.version.to_le_bytes())?;
        output.write_all(self.data)?;

        Ok(())
    }
}
