use std::collections::HashMap;

use crate::sync::Synchronized;

/// Thread-safe hash table type (protected by mutex)
pub type HashTable<T, K> = Synchronized<HashMap<T, K>>;
