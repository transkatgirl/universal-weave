//! Utilities for versioning serialized binary data

/// A set of bytes accompanied by file header information
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
    /// Serializes the header and contents into the specified byte array
    pub fn to_bytes(self, output: &mut Vec<u8>) {
        output.extend(self.format_identifier);
        output.extend(self.version.to_le_bytes());
        output.extend(self.data);
    }
}
