pub mod adapter;
pub mod disk;
pub mod engine;
pub mod memory;
pub mod mvcc;

pub use adapter::StorageAdapter;
pub use disk::OnDiskStorage;
pub use engine::{StorageEngine, StorageError};
pub use memory::InMemoryStorage;
pub use mvcc::{VersionChain, VersionedValue};
