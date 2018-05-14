//! Ethereum blooms database
//!
//! zero allocation
//! zero copying

#[macro_use]
extern crate arrayref;
extern crate byteorder;
extern crate ethbloom;
extern crate memmap;

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
