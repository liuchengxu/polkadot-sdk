use hash_db::{AsHashDB, HashDB, HashDBRef, Hasher, Prefix};
use sp_state_machine::TrieBackendStorage;
use sp_trie::{DBValue, PrefixedMemoryDB};
use std::marker::PhantomData;
use std::sync::Arc;

/// Similar to `Ephemeral` in trie-backend-essence, but uses persistent overlay.
pub(crate) struct MutableTrie<'a, S: 'a + TrieBackendStorage<H>, H: 'a + Hasher> {
	storage: &'a S,
	// TODO: persistent overlay
	overlay: &'a mut PrefixedMemoryDB<H>,
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: 'a + Hasher> AsHashDB<H, DBValue>
	for MutableTrie<'a, S, H>
{
	fn as_hash_db<'b>(&'b self) -> &'b (dyn HashDB<H, DBValue> + 'b) {
		self
	}
	fn as_hash_db_mut<'b>(&'b mut self) -> &'b mut (dyn HashDB<H, DBValue> + 'b) {
		self
	}
}

impl<'a, S: TrieBackendStorage<H>, H: Hasher> MutableTrie<'a, S, H> {
	pub fn new(storage: &'a S, overlay: &'a mut PrefixedMemoryDB<H>) -> Self {
		MutableTrie { storage, overlay }
	}
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: Hasher> hash_db::HashDB<H, DBValue>
	for MutableTrie<'a, S, H>
{
	fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
		HashDB::get(self.overlay, key, prefix).or_else(|| {
			self.storage.get(key, prefix).unwrap_or_else(|e| {
				log::warn!(target: "trie", "Failed to read from DB: {}", e);
				None
			})
		})
	}

	fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
		HashDB::get(self, key, prefix).is_some()
	}

	fn insert(&mut self, prefix: Prefix, value: &[u8]) -> H::Out {
		HashDB::insert(self.overlay, prefix, value)
	}

	fn emplace(&mut self, key: H::Out, prefix: Prefix, value: DBValue) {
		HashDB::emplace(self.overlay, key, prefix, value)
	}

	fn remove(&mut self, key: &H::Out, prefix: Prefix) {
		HashDB::remove(self.overlay, key, prefix)
	}
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: Hasher> HashDBRef<H, DBValue> for MutableTrie<'a, S, H> {
	fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
		HashDB::get(self, key, prefix)
	}

	fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
		HashDB::contains(self, key, prefix)
	}
}
