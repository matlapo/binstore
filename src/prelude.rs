use std::mem;
use std::io::{Seek, SeekFrom};

// Re-export everything in the error module.
pub use crate::error::*;

/// A hashed key as they are stored in buckets.
pub type HashedKey = u64;

/// The type of the Values associated with each Key
pub type Value = u128;

/// The number of bits in a key.
pub const HASHED_KEY_SIZE: usize = mem::size_of::<HashedKey>();

/// The magic number used to identify a binstore's bucket.
pub const MAGIC: u32 = 0x594e4e4a;

/// The current version of the binstore file format.
pub const VERSION: u32 = 0;

/// The default step from one entry to the next in the sparse index.
pub const DEFAULT_SPARSE_INDEX_STEP: usize =
    4096 / (HASHED_KEY_SIZE + mem::size_of::<i64>());

/// The level of compression for LZ4.
pub const COMPRESSION_LEVEL: u32 = 10;

/// Return the current offset in a file.
pub fn tell<S: Seek>(s: &mut S) -> Result<u64> {
    let offset = s.seek(SeekFrom::Current(0))?;
    return Ok(offset);
}
