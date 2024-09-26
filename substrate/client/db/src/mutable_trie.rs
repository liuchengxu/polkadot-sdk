use crate::DbHash;
use hash_db::{AsHashDB, HashDB, HashDBRef, Hasher, Prefix};
use sp_database::{Database, Transaction};
use sp_state_machine::TrieBackendStorage;
use sp_trie::{DBValue, PrefixedMemoryDB};
use std::marker::PhantomData;
use std::sync::Arc;

/// Similar to `Ephemeral` in trie-backend-essence, but uses persistent overlay.
pub(crate) struct MutableTrie<'a, S: 'a + TrieBackendStorage<H>, H: 'a + Hasher> {
	storage: &'a S,
	persistent_overlay: Arc<dyn Database<DbHash>>,
	_phantom: PhantomData<H>,
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
	pub fn new(storage: &'a S, persistent_overlay: Arc<dyn Database<DbHash>>) -> Self {
		Self { storage, persistent_overlay, _phantom: Default::default() }
	}
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: Hasher> hash_db::HashDB<H, DBValue>
	for MutableTrie<'a, S, H>
{
	fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
		let db_key = sp_trie::prefixed_key::<H>(key, prefix);

		self.persistent_overlay.get(crate::columns::STATE, &db_key).or_else(|| {
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
		let key = H::hash(value);

		let prefixed_key = sp_trie::prefixed_key::<H>(&key, prefix);
		let mut tx = Transaction::new();
		tx.set(crate::columns::STATE, &prefixed_key, value);

		println!("[insert] tx: {tx:?}");
		self.persistent_overlay.commit(tx).unwrap();

		key
	}

	fn emplace(&mut self, key: H::Out, prefix: Prefix, value: DBValue) {
		let key = sp_trie::prefixed_key::<H>(&key, prefix);
		let mut tx = Transaction::new();
		tx.set(crate::columns::STATE, &key, &value);
		println!("[emplace] tx: {tx:?}");
		self.persistent_overlay.commit(tx).unwrap();
	}

	fn remove(&mut self, key: &H::Out, prefix: Prefix) {
		let key = sp_trie::prefixed_key::<H>(&key, prefix);
		let mut tx = Transaction::new();
		tx.remove(crate::columns::STATE, &key);
		println!("[remove] tx: {tx:?}");
		self.persistent_overlay.commit(tx).unwrap();
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
