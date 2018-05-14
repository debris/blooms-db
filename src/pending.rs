use std::{fs, io};
use std::io::{Seek, SeekFrom, Write, Read};
use byteorder::{WriteBytesExt, ReadBytesExt, LittleEndian};
use ethbloom;

pub struct Pending {
	file: fs::File,
}

impl Pending {
	pub fn append<'a, B>(&mut self, index: usize, bloom: B) -> io::Result<()> where ethbloom::BloomRef<'a>: From<B> {
		self.file.write_u64::<LittleEndian>(index as u64)?;
		self.file.write_all(ethbloom::BloomRef::from(bloom).data())
	}

	pub fn flush(&mut self) -> io::Result<()> {
		self.file.sync_all()
	}

	pub fn clear(&mut self) -> io::Result<()> {
		self.file.seek(SeekFrom::Start(0))?;
		self.file.set_len(0)?;
		self.file.sync_all()
	}

	pub fn iterator(&self) -> io::Result<PendingIterator> {
		let mut file_ref = &self.file;
		file_ref.seek(SeekFrom::Start(0))?;
		let iter = PendingIterator {
			file: file_ref,
		};
		Ok(iter)
	}
}

pub struct PendingIterator<'a> {
	file: &'a fs::File,
}

impl<'a> Iterator for PendingIterator<'a> {
	type Item = io::Result<(usize, ethbloom::Bloom)>;

	fn next(&mut self) -> Option<Self::Item> {
		let index = match self.file.read_u64::<LittleEndian>() {
			Ok(index) => index,
			Err(_) => return None,
		};

		let mut bloom = ethbloom::Bloom::default();
		match self.file.read_exact(&mut bloom) {
			Ok(_) => Some(Ok((index as usize, bloom))),
			Err(err) => Some(Err(err)),
		}
	}
}
