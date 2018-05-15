//! Ethereum blooms database
//!
//! zero allocation
//! zero copying

extern crate byteorder;
extern crate ethbloom;

#[cfg(test)]
extern crate tempdir;

mod db;
mod file;
mod pending;

pub use db::{Database, DatabaseIterator};

#[cfg(test)]
mod tests {
	#[test]
	fn it_works() {
		assert_eq!(2 + 2, 4);
	}
}
