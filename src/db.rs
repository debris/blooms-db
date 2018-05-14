use std::io;
use ethbloom;
use file::{File, FileIterator};
use pending::Pending;

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
}

impl Database {
	/// Insert consecutive blooms into database starting with positon from.
	pub fn insert_blooms<'a, B>(&'a mut self, from: usize, blooms: impl Iterator<Item = B>) -> io::Result<()>
	where ethbloom::BloomRef<'a>: From<B> {
		for (index, bloom) in (from..).into_iter().zip(blooms) {
			self.pending.append(index, bloom)?;
		}
		self.pending.flush()
	}

	/// Flush pending blooms.
	pub fn flush(&mut self) -> io::Result<()> {
		for tuple in self.pending.iterator()? {
			let (index, bloom) = tuple?;
			// index / 256
			let top_pos = index >> 8;
			// index / 16
			let mid_pos = index >> 4;
			// index
			let bot_pos = index;

			// constant forks make lead to increased ration of false positives in bloom filters
			// since we do not rebuild top or mid level, but we should not be worried about that
			// most of the time events at block n(a) occur also on block n(b) or n+1(b)
			self.top.accrue_bloom(top_pos, &bloom);
			self.mid.accrue_bloom(mid_pos, &bloom);
			self.bot.replace_bloom(bot_pos, &bloom);
		}
		self.top.flush()?;
		self.mid.flush()?;
		self.bot.flush()?;
		self.pending.clear()
	}

	/// Returns an iterator yielding all indexes containing given bloom.
	pub fn iterate_matching<'a, B>(&'a self, from: usize, to: usize, bloom: B) -> DatabaseIterator<'a, File>
	where ethbloom::BloomRef<'a>: From<B> {
		DatabaseIterator {
			top: self.top.iterator(),
			mid: self.mid.iterator(),
			bot: self.bot.iterator(),
			state: IteratorState::Top,
			from,
			to,
			// from / 256 * 256
			index: (from & (usize::max_value() ^ 0x11)),
			bloom: bloom.into(),
		}
	}
}

/// Blooms database iterator
pub struct DatabaseIterator<'a, D> where D: 'a {
	top: FileIterator<'a, D>,
	mid: FileIterator<'a, D>,
	bot: FileIterator<'a, D>,
	state: IteratorState,
	from: usize,
	to: usize,
	index: usize,
	bloom: ethbloom::BloomRef<'a>,
}

/// Database iterator state.
enum IteratorState {
	/// Iterator should read top level bloom
	Top,
	/// Iterator should read mid level bloom `x` more times
	Mid(usize),
	/// Iterator should read mid level bloom `mid` more times
	/// and bot level `mix * 16 + bot` times
	Bot { mid: usize, bot: usize },
}

impl<'a, D> Iterator for DatabaseIterator<'a, D> where D: AsRef<[u8]> {
	type Item = usize;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if self.index > self.to {
				return None;
			}

			self.state = match self.state {
				IteratorState::Top => {
					if self.top.next()?.contains_bloom(self.bloom) {
						IteratorState::Mid(16)
					} else {
						self.index += 256;
						self.mid.advance(16);
						self.bot.advance(256);
						IteratorState::Top
					}
				},
				IteratorState::Mid(left) => {
					if left == 0 {
						IteratorState::Top
					} else if (self.index + 16) >= self.from && self.mid.next()?.contains_bloom(self.bloom) {
						IteratorState::Bot { mid: left - 1, bot: 16 }
					} else {
						self.index += 16;
						self.bot.advance(16);
						IteratorState::Mid(left - 1)
					}
				},
				IteratorState::Bot { mid, bot } => {
					if bot == 0 {
						IteratorState::Mid(mid)
					} else if self.index >= self.from && self.bot.next()?.contains_bloom(self.bloom) {
						let result = self.index;
						self.index += 1;
						self.state = IteratorState::Bot { mid, bot: bot - 1 };
						return Some(result);
					} else {
						IteratorState::Bot { mid, bot: bot - 1 }
					}
				}
			}
		}
	}
}
