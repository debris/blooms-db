use std::io;
use std::path::{Path, PathBuf};

use ethbloom;

use VERSION;
use file::{File, FileIterator};
use meta::{Meta, read_meta, save_meta};
use pending::Pending;

/// Bloom positions in database files.
#[derive(Debug)]
struct Positions {
	top: u64,
	mid: u64,
	bot: u64
}

impl Positions {
	fn from_index(index: u64) -> Self {
		Positions {
			top: index >> 8,
			mid: index >> 4,
			bot: index,
		}
	}
}

/// Blooms database.
pub struct Database {
	/// Top level bloom file
	///
	/// Every bloom represents 16 blooms on mid level
	top: File,
	/// Mid level bloom file
	///
	/// Every bloom represents 16 blooms on bot level
	mid: File,
	/// Bot level bloom file
	///
	/// Every bloom is an ethereum header bloom
	bot: File,
	/// Pending changes
	///
	/// Inserted blooms are always appended to this file
	pending: Pending,
	/// Datbase directory
	path: PathBuf,
}

impl Database {
	/// Opens blooms database.
	pub fn open<P>(path: P) -> io::Result<Database> where P: AsRef<Path> {
		let path = path.as_ref();
		let database = Database {
			top: File::open(path.join("top.bdb"))?,
			mid: File::open(path.join("mid.bdb"))?,
			bot: File::open(path.join("bot.bdb"))?,
			pending: Pending::open(path.join("pending.bdb"))?,
			path: path.to_path_buf(),
		};

		match read_meta(path.join("meta.bdb")) {
			Ok(meta) => {
				let pending_hash = database.pending.hash()?;
				if pending_hash != meta.pending_hash {
					return Err(io::Error::new(io::ErrorKind::InvalidData, "Malformed pending file"));
				}
			},
			Err(ref err) if err.kind() == io::ErrorKind::NotFound => {},
			Err(err) => return Err(err),
		}

		Ok(database)
	}

	/// Insert consecutive blooms into database starting with positon from.
	pub fn insert_blooms<'a, B>(&'a mut self, from: u64, blooms: impl Iterator<Item = B>) -> io::Result<()>
	where ethbloom::BloomRef<'a>: From<B> {
		for (index, bloom) in (from..).into_iter().zip(blooms) {
			self.pending.append(index, bloom)?;
		}
		self.pending.flush()?;
		self.flush_meta()
	}

	/// Flush pending blooms.
	pub fn flush(&mut self) -> io::Result<()> {
		for tuple in self.pending.iterator()? {
			let (index, bloom) = tuple?;
			let pos = Positions::from_index(index);

			// constant forks make lead to increased ration of false positives in bloom filters
			// since we do not rebuild top or mid level, but we should not be worried about that
			// most of the time events at block n(a) occur also on block n(b) or n+1(b)
			self.top.accrue_bloom(pos.top, &bloom)?;
			self.mid.accrue_bloom(pos.mid, &bloom)?;
			self.bot.replace_bloom(pos.bot, &bloom)?;
		}
		self.top.flush()?;
		self.mid.flush()?;
		self.bot.flush()?;
		self.pending.clear()?;
		self.flush_meta()
	}

	/// Returns an iterator yielding all indexes containing given bloom.
	pub fn iterate_matching<'a, B>(&'a self, from: u64, to: u64, bloom: B) -> io::Result<DatabaseIterator<'a>>
	where ethbloom::BloomRef<'a>: From<B> {
		let index = from / 256 * 256;
		let pos = Positions::from_index(index);

		let iter = DatabaseIterator {
			top: self.top.iterator_from(pos.top)?,
			mid: self.mid.iterator_from(pos.mid)?,
			bot: self.bot.iterator_from(pos.bot)?,
			state: IteratorState::Top,
			from,
			to,
			index,
			bloom: bloom.into(),
		};

		Ok(iter)
	}

	fn flush_meta(&self) -> io::Result<()> {
		let meta = Meta {
			version: VERSION,
			pending_hash: self.pending.hash()?
		};

		save_meta(self.path.join("meta.bdb"), &meta)
	}
}

/// Blooms database iterator
pub struct DatabaseIterator<'a> {
	top: FileIterator<'a>,
	mid: FileIterator<'a>,
	bot: FileIterator<'a>,
	state: IteratorState,
	from: u64,
	to: u64,
	index: u64,
	bloom: ethbloom::BloomRef<'a>,
}

/// Database iterator state.
#[derive(Debug)]
enum IteratorState {
	/// Iterator should read top level bloom
	Top,
	/// Iterator should read mid level bloom `x` more times
	Mid(usize),
	/// Iterator should read mid level bloom `mid` more times
	/// and bot level `mix * 16 + bot` times
	Bot { mid: usize, bot: usize },
}

impl<'a> Iterator for DatabaseIterator<'a> {
	type Item = io::Result<u64>;

	fn next(&mut self) -> Option<Self::Item> {
		macro_rules! try_o {
			($expr: expr) => {
				match $expr {
					Err(err) => return Some(Err(err)),
					Ok(ok) => ok,
				}
			}
		}

		loop {
			if self.index > self.to {
				return None;
			}

			self.state = match self.state {
				IteratorState::Top => {
					if try_o!(self.top.next()?).contains_bloom(self.bloom) {
						IteratorState::Mid(16)
					} else {
						self.index += 256;
						try_o!(self.mid.advance(16));
						try_o!(self.bot.advance(256));
						IteratorState::Top
					}
				},
				IteratorState::Mid(left) => {
					if left == 0 {
						IteratorState::Top
					} else if try_o!(self.mid.next()?).contains_bloom(self.bloom) && self.index + 16 >= self.from {
						IteratorState::Bot { mid: left - 1, bot: 16 }
					} else {
						self.index += 16;
						try_o!(self.bot.advance(16));
						IteratorState::Mid(left - 1)
					}
				},
				IteratorState::Bot { mid, bot } => {
					if bot == 0 {
						IteratorState::Mid(mid)
					} else if try_o!(self.bot.next()?).contains_bloom(self.bloom) && self.index >= self.from {
						let result = self.index;
						self.index += 1;
						self.state = IteratorState::Bot { mid, bot: bot - 1 };
						return Some(Ok(result));
					} else {
						self.index += 1;
						IteratorState::Bot { mid, bot: bot - 1 }
					}
				}
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use ethbloom::Bloom;
	use tempdir::TempDir;
	use super::Database;

	#[test]
	fn test_database() {
		let tempdir = TempDir::new("").unwrap();
		let mut database = Database::open(tempdir.path()).unwrap();
		database.insert_blooms(0, vec![Bloom::from(0), Bloom::from(0x01), Bloom::from(0x10), Bloom::from(0x11)].iter()).unwrap();
		database.flush().unwrap();

		let matches = database.iterate_matching(0, 3, &Bloom::from(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![0, 1, 2, 3]);

		let matches = database.iterate_matching(0, 4, &Bloom::from(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![0, 1, 2, 3]);

		let matches = database.iterate_matching(1, 3, &Bloom::from(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![1, 2, 3]);

		let matches = database.iterate_matching(1, 2, &Bloom::from(0)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![1, 2]);

		let matches = database.iterate_matching(0, 3, &Bloom::from(0x01)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![1, 3]);

		let matches = database.iterate_matching(0, 3, &Bloom::from(0x10)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![2, 3]);

		let matches = database.iterate_matching(2, 2, &Bloom::from(0x10)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![2]);
	}

	#[test]
	fn test_database2() {
		let tempdir = TempDir::new("").unwrap();
		let mut database = Database::open(tempdir.path()).unwrap();
		database.insert_blooms(254, vec![Bloom::from(0x100), Bloom::from(0x01), Bloom::from(0x10), Bloom::from(0x11)].iter()).unwrap();
		database.flush().unwrap();

		let matches = database.iterate_matching(0, 257, &Bloom::from(0x01)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![255, 257]);

		let matches = database.iterate_matching(0, 258, &Bloom::from(0x100)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![254]);

		let matches = database.iterate_matching(0, 256, &Bloom::from(0x01)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![255]);

		let matches = database.iterate_matching(255, 255, &Bloom::from(0x01)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![255]);

		let matches = database.iterate_matching(256, 256, &Bloom::from(0x10)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![256]);

		let matches = database.iterate_matching(256, 257, &Bloom::from(0x10)).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(matches, vec![256, 257]);
	}
}
