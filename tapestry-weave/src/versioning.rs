use std::borrow::Cow;

const FORMAT_IDENTIFIER: &[u8] = b"VersionedTapestryWeave";
const FORMAT_IDENTIFIER_LENGTH: usize = FORMAT_IDENTIFIER.len();
const VERSION_LENGTH: usize = 8;
const DATA_OFFSET: usize = FORMAT_IDENTIFIER_LENGTH + VERSION_LENGTH;

pub struct VersionedBytes<'a> {
    pub version: u64,
    pub data: Cow<'a, [u8]>,
}

impl<'a> VersionedBytes<'a> {
    pub fn from_bytes(value: &'a [u8]) -> Option<Self> {
        if value.starts_with(FORMAT_IDENTIFIER) && value.len() >= DATA_OFFSET {
            let (version_bytes, data) =
                value[FORMAT_IDENTIFIER_LENGTH..].split_at(size_of::<u64>());

            Some(Self {
                version: u64::from_le_bytes(version_bytes.try_into().unwrap()),
                data: Cow::Borrowed(data),
            })
        } else {
            None
        }
    }
    pub fn to_bytes(self) -> (&'static [u8], [u8; 8], Cow<'a, [u8]>) {
        (FORMAT_IDENTIFIER, self.version.to_le_bytes(), self.data)
    }
    pub fn to_byte_iterator(self) -> impl Iterator<Item = u8> {
        let byte_set = self.to_bytes();

        byte_set
            .0
            .iter()
            .copied()
            .chain(byte_set.1.into_iter().chain(match byte_set.2 {
                Cow::Owned(b) => b,
                Cow::Borrowed(b) => b.to_owned(),
            }))
    }
}
