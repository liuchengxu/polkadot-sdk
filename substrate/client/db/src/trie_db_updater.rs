use crate::{columns, DbHash};
use hash_db::{AsHashDB, HashDB, HashDBRef, Hasher, Prefix};
use sp_database::{Change, Database, Transaction};
use sp_state_machine::TrieBackendStorage;
use sp_trie::DBValue;
use std::{marker::PhantomData, sync::Arc};

/// Updates the state trie in the database directly.
///
/// The storage updates directly happen in the database, instead of being collected into
/// a `PrefixedMemoryDB` and then applied to the database later.
///
/// Similar to `Ephemeral` in trie-backend-essence, but uses persistent overlay.
pub(crate) struct TrieDbUpdater<'a, S: 'a + TrieBackendStorage<H>, H: 'a + Hasher> {
	/// Old state storage.
	storage: &'a S,
	/// State DB.
	persistent_overlay: Arc<dyn Database<DbHash>>,
	_phantom: PhantomData<H>,
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: 'a + Hasher> AsHashDB<H, DBValue>
	for TrieDbUpdater<'a, S, H>
{
	fn as_hash_db<'b>(&'b self) -> &'b (dyn HashDB<H, DBValue> + 'b) {
		self
	}
	fn as_hash_db_mut<'b>(&'b mut self) -> &'b mut (dyn HashDB<H, DBValue> + 'b) {
		self
	}
}

impl<'a, S: TrieBackendStorage<H>, H: Hasher> TrieDbUpdater<'a, S, H> {
	pub fn new(storage: &'a S, persistent_overlay: Arc<dyn Database<DbHash>>) -> Self {
		Self { storage, persistent_overlay, _phantom: Default::default() }
	}
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: Hasher> hash_db::HashDB<H, DBValue>
	for TrieDbUpdater<'a, S, H>
{
	fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
		let db_key = sp_trie::prefixed_key::<H>(key, prefix);

		self.persistent_overlay.get(columns::STATE, &db_key).or_else(|| {
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

		let db_key = sp_trie::prefixed_key::<H>(&key, prefix);
		let tx = Transaction(vec![Change::Set(columns::STATE, db_key, value.to_vec())]);
		self.persistent_overlay.commit(tx).unwrap();

		key
	}

	fn emplace(&mut self, key: H::Out, prefix: Prefix, value: DBValue) {
		let key = sp_trie::prefixed_key::<H>(&key, prefix);
		let tx = Transaction(vec![Change::Set(columns::STATE, key, value)]);
		self.persistent_overlay.commit(tx).unwrap();
	}

	fn remove(&mut self, key: &H::Out, prefix: Prefix) {
		let key = sp_trie::prefixed_key::<H>(&key, prefix);
		let tx = Transaction(vec![Change::Remove(columns::STATE, key)]);
		self.persistent_overlay.commit(tx).unwrap();
	}
}

impl<'a, S: 'a + TrieBackendStorage<H>, H: Hasher> HashDBRef<H, DBValue>
	for TrieDbUpdater<'a, S, H>
{
	fn get(&self, key: &H::Out, prefix: Prefix) -> Option<DBValue> {
		HashDB::get(self, key, prefix)
	}

	fn contains(&self, key: &H::Out, prefix: Prefix) -> bool {
		HashDB::contains(self, key, prefix)
	}
}
