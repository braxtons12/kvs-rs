//! A Basic Key Value Store
//!
#![deny(clippy::all, clippy::pedantic, clippy::cargo)]
#![deny(missing_docs)]
#![allow(clippy::clippy::multiple_crate_versions)]

extern crate lazy_static;
extern crate serde;
extern crate serde_json;
extern crate thiserror;
#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_json;
#[macro_use]
extern crate slog_scope;
extern crate slog_stdlog;
extern crate slog_term;

mod errors;

use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use serde_json::{to_string, Deserializer};
use slog::{Drain, Logger};
use structopt::StructOpt;

pub use errors::KvStoreError;

/// `KvStore` alias for `std::result::Result<T, KvStoreError>`
pub type Result<T> = std::result::Result<T, KvStoreError>;

static FILE_SIZE_ENTRIES: usize = 1000;
static COMPACTION_FILES_THRESHOLD: usize = 20;
static LOG_CHANNEL_SIZE: usize = 10_000_000;
static NUM_WRITES_FOR_COMPACTION: usize = FILE_SIZE_ENTRIES * COMPACTION_FILES_THRESHOLD;
lazy_static! {
	static ref FILE_EXT: String = "kvs".to_owned();
}

lazy_static! {
	static ref SWAP_FILE_POSTFIX: String = "_swap".to_owned();
}

/// Creates the `slog::Logger` for the library/application
#[must_use]
pub fn setup_loggers() -> Logger {
	let time = chrono::Utc::now()
		.format("[%Y-%m-%d][%H:%M:%S]")
		.to_string();
	let mut dir = dirs::data_local_dir().expect("Failed to open local data directory for logging");
	dir.push("kvs");
	std::fs::create_dir_all(dir.clone()).expect("Failed to create logging directory");
	dir.push("KVSLog".to_string() + " " + time.as_str());
	let log_file = OpenOptions::new()
		.create(true)
		.write(true)
		.truncate(true)
		.open(dir)
		.expect("Failed to open log file");
	let term_decorator = slog_term::TermDecorator::new()
		.force_color()
		.stderr()
		.build();
	let term_drain = slog_term::FullFormat::new(term_decorator)
		.use_utc_timestamp()
		.use_original_order()
		.build()
		.fuse();
	let term_async = slog_async::Async::new(term_drain).build().fuse();

	let json_drain = slog_json::Json::new(log_file)
		.set_newlines(true)
		.set_flush(true)
		.set_pretty(true)
		.add_default_keys()
		.build()
		.fuse();
	// assume 50x `FILE_SIZE_ENTRIES` will be a good default for logging channel size
	let json_async = slog_async::Async::new(json_drain)
		.chan_size(LOG_CHANNEL_SIZE)
		.build()
		.fuse();

	let logger = slog::Logger::root(
		slog::Duplicate::new(term_async, json_async).fuse(),
		o!("| version" => env!("CARGO_PKG_VERSION")),
	);
	let guard = slog_scope::set_global_logger(logger.clone());
	guard.cancel_reset();
	slog_stdlog::init().ok();
	logger
}

/// Used to communicate the locations of a key, if it is present
pub enum ContainedLocation {
	/// The key is not present
	NotContained,
	/// The key is present
	Contained {
		/// The file with the most recent entry for the key
		file_id: usize,
		/// The entry index in the file for the most recent entry for the key
		file_index: usize,
		/// The in-memory index index for the key
		index_index: usize,
	},
}

/// `KvStore` Index storing an in-memory representation of the database state
#[derive(Default, Debug, Serialize, Deserialize)]
pub struct Index {
	/// The index of Key-Value pair entries in the database
	entries: Vec<IndexEntry>,
	/// The current file to write updates to
	current_file_id: usize,
	/// The current index to write updates at
	current_file_index: usize,
	/// The current number of writes since the last compaction
	writes_since_compaction: usize,
	/// The root path of the database (the folder it is stored in)
	root_path: PathBuf,
}

impl Index {
	/// Creates a new `Index`
	#[must_use]
	pub fn new() -> Self {
		Index::default()
	}

	/// Checks if the given key is contained in the database
	///
	/// # Arguments
	/// * `key` - The key to lookup in the database
	#[must_use]
	#[allow(clippy::clippy::needless_pass_by_value)]
	pub fn contains(&self, key: String) -> ContainedLocation {
		trace!("Searching for key {}", key.clone());
		for i in 0..self.entries.len() {
			let entry = self.entries[i].clone();
			if entry.key == key {
				return ContainedLocation::Contained {
					file_id: entry.file_id,
					file_index: entry.index,
					index_index: i,
				};
			}
		}

		ContainedLocation::NotContained
	}

	/// Writes the given key-value pair to the current database file, updating the index
	/// appropriately
	///
	/// # Arguments
	/// * `key` - The key to update in the database
	/// * `value` - The value to associate with the given key
	///
	/// # Errors
	/// Returns an error if opening the file fails, serialization fails, or `command` is not
	/// `Commands::Set`
	pub fn set_in_database(&mut self, key: String, value: String) -> Result<()> {
		trace!("Setting KV pair in DB: key: {}, value: {}", key, value);
		let mut path = self.root_path.clone();
		path.push(self.current_file_id.to_string());
		path = path.with_extension(FILE_EXT.to_string());

		// open the file
		let file = OpenOptions::new().create(true).append(true).open(path)?;
		let mut writer =
			BufWriter::with_capacity(FILE_SIZE_ENTRIES * std::mem::size_of::<Commands>(), file);
		let command = Commands::Set {
			key: key.clone(),
			value,
		};
		// write to the database file
		writer.write_all(to_string(&command)?.as_bytes())?;
		writer.flush()?;
		trace!("KV pair written to DB file {}", self.current_file_id);

		// if the index already contained this key, update the entry in the index
		if let ContainedLocation::Contained {
			file_id: _,
			file_index: _,
			index_index,
		} = self.contains(key.clone())
		{
			self.entries[index_index].file_id = self.current_file_id;
			self.entries[index_index].index = self.current_file_index;
		}
		//otherwise, add the entry to the index
		else {
			self.entries.push(IndexEntry::new(
				key,
				self.current_file_id,
				self.current_file_index,
			));
		}
		self.current_file_index += 1;
		self.writes_since_compaction += 1;

		//check if we need to compact
		if self.writes_since_compaction >= NUM_WRITES_FOR_COMPACTION {
			trace!("Reached compaction threshold. Compacting files");
			self.compact()?;
			self.writes_since_compaction = 0;
		}

		// check if we need to move to a new file
		if self.current_file_index >= FILE_SIZE_ENTRIES {
			trace!(
				"Reached max file size for file {}; Switching writing to next DB File {}",
				self.current_file_id,
				self.current_file_id + 1
			);
			self.current_file_index = 0;
			self.current_file_id += 1;
		}

		Ok(())
	}

	/// Updates the index with the given set command
	///
	/// # Arguments
	/// * `key` - The key to update
	fn set_in_index(&mut self, key: String) {
		trace!("Setting KV pair in index: key: {}", key.clone());
		if let ContainedLocation::Contained {
			file_id: _,
			file_index: _,
			index_index,
		} = self.contains(key.clone())
		{
			trace!("Index already contained KV pair, updating entry in index");
			self.entries[index_index].file_id = self.current_file_id;
			self.entries[index_index].index = self.current_file_index;
		} else {
			trace!("Index didn't contain KV pair, adding new entry to index");
			self.entries.push(IndexEntry::new(
				key,
				self.current_file_id,
				self.current_file_index,
			));
		}
		self.current_file_index += 1;

		if self.current_file_index >= FILE_SIZE_ENTRIES {
			trace!(
				"Reached max file size for file {}; Incrementing DB file pointer in Index to {}",
				self.current_file_id,
				self.current_file_id + 1
			);
			self.current_file_index = 0;
			self.current_file_id += 1;
		}
	}

	/// Searches the database for the given key, and if it is found, returns the associated value
	///
	/// # Arguments
	/// * `key` - The key to lookup in the databse
	///
	/// # Errors
	/// Returns an error if the database file fails to open, or the data fails to read,
	/// deserialize, or is corrupted
	#[allow(clippy::clippy::needless_pass_by_value)]
	pub fn get(&self, key: String) -> Result<Option<String>> {
		trace!("Retrieving value for key: {}", key.clone());
		// if the key is found, read its corresponding value from the database file
		if let ContainedLocation::Contained {
			file_id,
			file_index,
			index_index: _,
		} = self.contains(key.clone())
		{
			trace!(
				"Index contained entry for key: {}, retrieving value from DB file {}",
				key.clone(),
				file_id
			);
			trace!(
				"Found entry for key: {} in index. file_id: {}, file_index: {}",
				key.clone(),
				file_id,
				file_index
			);
			let mut path = self.root_path.clone();
			path.push(file_id.to_string());
			path = path.with_extension(FILE_EXT.to_string());

			// open the file
			let file = OpenOptions::new().read(true).open(path)?;
			let reader =
				BufReader::with_capacity(FILE_SIZE_ENTRIES * std::mem::size_of::<Commands>(), file);
			let entries: Vec<Commands> = Deserializer::from_reader(reader)
				.into_iter()
				.collect::<std::result::Result<Vec<Commands>, serde_json::Error>>(
			)?;

			trace!("read entries from disk. entries length: {}", entries.len());
			match &entries[file_index] {
				// return the value if it is
				Commands::Set { key: _, value } => Ok(Some(value.clone())),
				// otherwise, the data is corrupted
				_ => Err(KvStoreError::new_db_data_corruption()),
			}
		}
		// otherwise, return that it's not present in the database
		else {
			trace!("Entry for key {} not found in index", key.clone());
			Ok(None)
		}
	}

	/// Removes the given key from the database if it is present
	///
	/// # Arguments
	/// * `key` - the key to remove from the database
	///
	/// # Errors
	/// Returns an error if the database file fails to open, serialization of the remove command
	/// fails, or the key is not found
	pub fn rm_from_database(&mut self, key: String) -> Result<()> {
		trace!("Removing entry for key {} from DB", key.clone());
		// if the database contains the given key, remove it from the database
		if let ContainedLocation::Contained {
			file_id: _,
			file_index: _,
			index_index,
		} = self.contains(key.clone())
		{
			trace!("Found entry for key {} in Index", key.clone());
			let mut path = self.root_path.clone();
			path.push(self.current_file_id.to_string());
			path = path.with_extension(FILE_EXT.to_string());

			// open the databse file
			let file = OpenOptions::new().create(true).append(true).open(path)?;
			let mut writer = BufWriter::new(file);
			let command = Commands::Rm { key };
			trace!(
				"Writing removal command to DB file {}",
				self.current_file_id
			);
			// write to the database file
			writer.write_all(to_string(&command)?.as_bytes())?;
			// update the index
			self.entries.remove(index_index);
			self.current_file_index += 1;
			self.writes_since_compaction += 1;

			//check if we need to compact
			if self.writes_since_compaction >= NUM_WRITES_FOR_COMPACTION {
				trace!("Reached compaction threshold. Compacting files");
				self.compact()?;
				self.writes_since_compaction = 0;
			}

			// check if we need to move to a new file
			if self.current_file_index >= FILE_SIZE_ENTRIES {
				trace!(
					"Reached max file size for file {}; Switching writing to next DB File {}",
					self.current_file_id,
					self.current_file_id + 1
				);
				self.current_file_index = 0;
				self.current_file_id += 1;
			}

			Ok(())
		}
		// otherwise, return an error
		else {
			trace!(
				"Removal failure: Entry for key {} not found in Index",
				key.clone()
			);
			Err(KvStoreError::new_remove_not_found())
		}
	}

	/// Updates the index with the given remove command
	///
	/// # Arguments
	/// * `key` - The key to remove
	#[allow(clippy::clippy::needless_pass_by_value)]
	fn rm_from_index(&mut self, key: String) {
		trace!("Removing entry for key {} from Index", key.clone());
		if let ContainedLocation::Contained {
			file_id: _,
			file_index: _,
			index_index,
		} = self.contains(key.clone())
		{
			trace!("Found entry for key {} in Index, removing from Index", key);
			self.entries.remove(index_index);
			self.current_file_index += 1;

			if self.current_file_index >= FILE_SIZE_ENTRIES {
				trace!(
					"Reached max file size for file {}; Incrementing DB file pointer in Index to {}",
					self.current_file_id,
					self.current_file_id + 1
				);
				self.current_file_index = 0;
				self.current_file_id += 1;
			}
		}
	}

	/// Opens the database at the given path, using the index cache if available, or rebuilding it
	/// from the database files if necessary
	///
	/// # Arguments
	/// * `path` - The path to open the database at
	///
	/// # Errors
	/// Returns an error if any file I/O fails during index rebuilding, or deserialization fails
	pub fn open(&mut self, path: PathBuf) -> Result<()> {
		trace!("Opening DB at path: {:#?}", path.clone());
		// build the path to the index cache
		let mut index_path = path.clone();
		index_path.push("index".to_owned());
		index_path = index_path.with_extension(FILE_EXT.to_string());

		// if the index cache exists, read it from disk.
		// Then, check disk contents and rebuild the remaining index if necessary
		//
		// TODO: impl `Drop` such that the index cache is written to disk on drop
		if let Ok(index_file) = OpenOptions::new().read(true).open(index_path) {
			trace!("Found index cache file. Updating Index from it");
			let mut reader = BufReader::new(index_file);
			let mut buffer =
				Vec::with_capacity(FILE_SIZE_ENTRIES * std::mem::size_of::<Commands>());
			reader.read_to_end(&mut buffer)?;

			let index = serde_json::from_str::<Index>(std::str::from_utf8(buffer.as_slice())?)?;
			self.entries = index.entries;
			self.current_file_id = index.current_file_id;
			self.current_file_index = index.current_file_index;
			self.root_path = path;

			trace!("Rebuilding remaining Index entries, if necessary");
			self.rebuild_if_necessary(false)
		}
		// Otherwise, rebuild the entire index from scratch
		else {
			trace!("Index cache not found, rebuilding Index from scratch");
			self.root_path = path;
			self.rebuild_if_necessary(true)
		}
	}

	/// Rebuilds the index, starting with the `current_file_id` and `current_file_index`, then progressing
	/// through subsequent database files as necessary
	///
	/// # Errors
	/// Returns an error on file I/O failure or data deserialization failure
	fn rebuild_if_necessary(&mut self, from_scratch: bool) -> Result<()> {
		trace!("Rebuilding index. from_scratch: {}", from_scratch);
		let entries = std::fs::read_dir(self.root_path.clone())?;
		let size = entries.count();
		// for every remaining entry, rebuild the index from that file
		let start_index = {
			if from_scratch {
				0
			} else {
				self.current_file_id
			}
		};

		// don't assume that `std::fs::read_dir` is giving us the entries in sorted
		// order. Iterate through the IDs we might need in order,
		// then read from them if they exist
		for i in start_index..size + start_index {
			let entries = std::fs::read_dir(self.root_path.clone())?;

			#[allow(clippy::clippy::explicit_counter_loop)]
			for entry in entries {
				let entry = entry?;
				let path = entry.path();

				if let Some(extension) = path.extension() {
					// check that the file is a kvs file AND it's not the index cache
					if extension.to_string_lossy() == FILE_EXT.as_str()
						&& path.to_string_lossy() != "index.".to_string() + FILE_EXT.as_str()
					{
						let mut rebuild_path = self.root_path.clone();
						rebuild_path.push(i.to_string());
						rebuild_path = rebuild_path.with_extension(FILE_EXT.to_string());
						if path.clone() == rebuild_path {
							trace!("Rebuilding from DB file {}", i);
							self.rebuild_from_file(path.clone())?;
						}
					}
				}
			}
		}
		trace!("Rebuild complete: Index length: {}", self.entries.len());
		//let last = self.get("key999".to_owned())?;
		//trace!("Last entry value: {}", last.unwrap());

		Ok(())
	}

	/// Rebuilds the index, starting with the `current_file_index`, then progressing through the
	/// file as necessary
	///
	/// # Errors
	/// Returns an error on file I/O failure or data deserialization failure
	fn rebuild_from_file(&mut self, path: PathBuf) -> Result<()> {
		if let Ok(file) = OpenOptions::new().read(true).open(path) {
			let reader = BufReader::new(file);
			let entries: Vec<Commands> = Deserializer::from_reader(reader)
				.into_iter()
				.collect::<std::result::Result<Vec<Commands>, serde_json::Error>>(
			)?;

			trace!(
				"Splitting file entries at current_file_index={}",
				self.current_file_index
			);
			let (_, subset) = entries.split_at(self.current_file_index);
			for entry in subset {
				match entry {
					Commands::Set { key, value: _ } => {
						trace!("Adding set command to index for key: {}", key.clone());
						self.set_in_index(key.clone());
					}
					Commands::Rm { key } => {
						trace!("Adding rm command to index for key: {}", key.clone());
						self.rm_from_index(key.clone());
					}
					_ => return Err(KvStoreError::new_db_data_corruption()),
				}
			}
		}
		Ok(())
	}

	/// Compacts the data into the minimum number of required log files
	///
	/// # Errors
	/// Returns an error if a file I/O, serialization, or deserialization error occurs
	pub fn compact(&mut self) -> Result<()> {
		trace!("Compacting Database");
		let mut file_id = 0_usize;

		let mut path = self.root_path.clone();
		path.push(file_id.to_string() + SWAP_FILE_POSTFIX.as_str());
		path = path.with_extension(FILE_EXT.to_string());
		let mut file = OpenOptions::new().create(true).append(true).open(path)?;
		let mut writer = BufWriter::new(file);

		let mut temp_index = Index::new();
		#[allow(clippy::clippy::needless_range_loop)]
		for i in 0..self.entries.len() {
			let key = self.entries[i].key.clone();
			let value = self.get(key.clone())?.unwrap();

			trace!(
				"Writing entry {}: key: {}, value: {} to DB swap file {}",
				i,
				key.clone(),
				value.clone(),
				file_id
			);
			let command = Commands::Set {
				key: key.clone(),
				value,
			};

			writer.write_all(to_string(&command)?.as_bytes())?;

			trace!("writing entry for key: {} to swap index", key.clone());
			temp_index.set_in_index(key);

			if temp_index.current_file_id != file_id {
				file_id = temp_index.current_file_id;
				trace!("Incrementing to next DB file: {}", file_id);
				path = self.root_path.clone();
				path.push(file_id.to_string() + SWAP_FILE_POSTFIX.as_str());
				path = path.with_extension(FILE_EXT.to_string());
				file = OpenOptions::new().create(true).append(true).open(path)?;
				writer = BufWriter::new(file);
			}
		}

		for i in 0..=file_id {
			trace!("Renaming DB swap file {} to active DB file", i);
			path = self.root_path.clone();
			path.push(i.to_string());
			path = path.with_extension(FILE_EXT.to_string());

			let mut old_path = self.root_path.clone();
			old_path.push(i.to_string() + SWAP_FILE_POSTFIX.as_str());
			old_path = old_path.with_extension(FILE_EXT.to_string());
			std::fs::remove_file(path.clone())?;
			std::fs::rename(old_path, path)?;
		}

		let entries = std::fs::read_dir(self.root_path.clone())?;
		let size = entries.count();
		let removal_id = file_id + 1;
		trace!("Removing unnessary DB files: {} and greater", removal_id);
		for i in removal_id..size + removal_id {
			let entries = std::fs::read_dir(self.root_path.clone())?;
			for entry in entries {
				let entry = entry?;
				let path = entry.path();
				let mut removal_path = self.root_path.clone();
				removal_path.push(i.to_string());
				removal_path = removal_path.with_extension(FILE_EXT.to_string());

				if path == removal_path {
					trace!("Removing unnessary DB file {}", i);
					std::fs::remove_file(path)?;
				}
			}
		}

		trace!("Updating active index to match compacted index");
		self.current_file_index = temp_index.current_file_index;
		self.current_file_id = temp_index.current_file_id;
		self.entries = temp_index.entries;

		Ok(())
	}
}

/// `KvStore` index entry storing the key and the value location
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexEntry {
	/// The key part of the Key-Value pair
	key: String,
	/// The file id the record is stored in
	file_id: usize,
	/// The index in the file at which the record is stored
	index: usize,
}

impl IndexEntry {
	/// Returns a new `IndexEntry` with the given parameters
	///
	/// # Arguments
	/// * `key` - The key for the entry
	/// * `file_id` - The file the most recent entry for the key is contained in
	/// * `index` - The entry index in the file at which the entry occurs
	#[must_use]
	pub fn new(key: String, file_id: usize, index: usize) -> Self {
		IndexEntry {
			key,
			file_id,
			index,
		}
	}
}

/// Basic Key-Value Store
///
/// Stores keys and values with a simple and naive indexing system
#[derive(Debug, Default)]
pub struct KvStore {
	index: Index,
}

impl KvStore {
	/// Creates a new `KvStore`
	#[must_use]
	pub fn new() -> Self {
		KvStore::default()
	}

	/// Sets the value associated with the given key to the given value
	///
	/// If the Key-Value pair is already present, this overwrites the present value,
	/// otherwise, this creates a new pair and stores the key and values
	///
	/// # Arguments
	/// * `key` - The key to lookup in the `KvStore`
	/// * `value` - The value to store in association with the given key
	///
	/// # Errors
	/// Returns an error if an IO operation causes the write to fail
	pub fn set(&mut self, key: String, value: String) -> Result<()> {
		self.index.set_in_database(key, value)
	}

	/// Retuns the value associated with the given key, if there is one, or `None`
	///
	/// # Arguments
	/// * `key` - The key to lookup in the `KvStore`
	///
	/// # Returns
	/// * `Some(String)` - The associated value, if the key is present
	/// * `None` - The given key is not in the `KvStore`
	///
	/// # Errors
	/// Returns an error if an IO operation causes the read to fail
	pub fn get(&mut self, key: String) -> Result<Option<String>> {
		self.index.get(key)
	}

	/// Removes the Key-Value pair for the given key, if there is one
	///
	/// # Arguments
	/// * `key` - The key to remove the Key-Value pair for
	///
	/// # Errors
	/// Returns an error if an IO operation causes the read to fail, or the key is not found in the
	/// database
	pub fn remove(&mut self, key: String) -> Result<()> {
		self.index.rm_from_database(key)
	}

	/// Opens a `KvStore` at the given path, creating the `KvStore` instance if one was not already
	/// present at the given path or intializing from disk otherwise
	///
	/// # Arguments
	/// * `path` - The path to open the `KvStore` at
	/// # Errors
	/// Returns an error if an IO operation causes the creation or initialization to fail
	#[allow(clippy::needless_pass_by_value)]
	pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
		let mut store = KvStore::new();
		store.index.open(path.into())?;
		Ok(store)
	}
}

/// `KvStore` operation types
#[derive(Clone, Debug, StructOpt, Deserialize, Serialize)]
pub enum Commands {
	/// Variant for the `get` command
	Get {
		/// the key to lookup in the `KvStore`
		#[structopt(name = "KEY", help = "The Key to lookup in the Key-Value Store")]
		key: String,
	},
	/// Variant for the `set` command
	Set {
		/// the key to modify in the `KvStore`
		#[structopt(name = "KEY", help = "The Key to but t associate with the given value")]
		key: String,
		/// the value to associate with the key
		#[structopt(name = "VALUE", help = "The value to store in the Key-Value Store")]
		value: String,
	},
	/// Variant for the `rm` command
	Rm {
		/// the key to remove from the `KvStore`
		#[structopt(
			name = "KEY",
			help = "The Key to remove the entry for in the Key-Value Store"
		)]
		key: String,
	},
}
