use std::io;

use thiserror::Error;

///
/// `KvStore` error type indicating why a database operation failed
/// Can be returned if any error occurs in database writes, reads, creation, or initialization
/// (opening of an already created database)
#[derive(Debug, Error)]
pub enum KvStoreError {
	/// Returned if an `io::Error` occurred when performing a database operation
	#[error("Failed to perform database operation: {source}")]
	IOError {
		/// The source of the error
		#[from]
		source: io::Error,
	},
	/// Returned if serialization or deserialization fails
	#[error("Failed to serialize or deserialize the database operation: {source}")]
	SerDeError {
		/// The source of the error
		#[from]
		source: serde_json::Error,
	},
	/// Returned if database read fails
	#[error("Failed to read from the database data: {source}")]
	DBReadError {
		/// The source of the error
		#[from]
		source: std::str::Utf8Error,
	},
	/// Returned if the key was not found when attempting a removal
	#[error("Key to remove not found")]
	RemoveNotFound,
	/// Returned if the incorrect command was given to the index for processing
	#[error("Incorrect command for operation")]
	IncorrectCommand,
	/// Returned if the database data was corrupted at the read-from index
	#[error("Database data corrupted")]
	DBDataCorruption,
	/// Returned if an unexpected error occurred
	#[error("An unexpected error occurred")]
	Other,
}

impl KvStoreError {
	/// Returns a `KvStoreError::IOError` from the given `io::Error`
	#[must_use]
	pub fn new_io(source: io::Error) -> Self {
		KvStoreError::IOError { source }
	}

	/// Returns a `KvStoreError::IOError` from the given `io::Error`
	#[must_use]
	pub fn new_serde(source: serde_json::Error) -> Self {
		KvStoreError::SerDeError { source }
	}

	/// Returns a `KvStoreError::IOError` from the given `io::Error`
	#[must_use]
	pub fn new_db_read(source: std::str::Utf8Error) -> Self {
		KvStoreError::DBReadError { source }
	}

	/// Returns a `KvStoreError::RemoveNotFound`
	#[must_use]
	pub fn new_remove_not_found() -> Self {
		KvStoreError::RemoveNotFound {}
	}

	/// Returns a `KvStoreError::IncorrectCommand`
	#[must_use]
	pub fn new_incorrect_command() -> Self {
		KvStoreError::IncorrectCommand {}
	}

	/// Returns a `KvStoreError::DBDataCorruption`
	#[must_use]
	pub fn new_db_data_corruption() -> Self {
		KvStoreError::DBDataCorruption {}
	}

	/// Returns a `KvStoreError::Other`
	#[must_use]
	pub fn new_other() -> Self {
		KvStoreError::Other {}
	}
}
