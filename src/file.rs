use std::io;
use {memmap, ethbloom};

/// Autoresizable memory mapped file.
pub struct File {
	/// Backing memory mapped file.
	mmap: memmap::MmapMut,
}

impl AsRef<[u8]> for File {
	fn as_ref(&self) -> &[u8] {
		self.mmap.as_ref()
	}
}

impl AsMut<[u8]> for File {
	fn as_mut(&mut self) -> &mut [u8] {
		self.mmap.as_mut()
	}
}

impl File {
	/// Read bloom at given position.
	pub fn read_bloom(&self, pos: usize) -> ethbloom::BloomRef {
		array_ref!(self.mmap.as_ref(), pos * 256, 256).into()
	}

	/// Accrue bloom into bloom at given position.
	pub fn accrue_bloom<'a, B>(&mut self, pos: usize, bloom: B) where ethbloom::BloomRef<'a>: From<B> {
		// TODO: implement BloomRefMut in ethbloom
		let mut old_bloom: ethbloom::Bloom = (self.read_bloom(pos).data() as &[u8]).into();
		old_bloom.accrue_bloom(bloom);
		let index = pos * 256;
		self.mmap.as_mut()[index..index + 256].copy_from_slice(old_bloom.data());
	}

	/// Replace bloom at given position with a new one.
	pub fn replace_bloom<'a, B>(&mut self, pos: usize, bloom: B) where ethbloom::BloomRef<'a>: From<B> {
		let index = pos * 256;
		self.mmap.as_mut()[index..index + 256].copy_from_slice(ethbloom::BloomRef::from(bloom).data());
	}

	/// Returns an iterator over file.
	pub fn iterator(&self) -> FileIterator<File> {
		FileIterator {
			read: 0,
			data: self
		}
	}

	/// Flush outstanding modifications to the disk
	pub fn flush(&self) -> io::Result<()> {
		self.mmap.flush()
	}
}

/// Iterator over blooms of a single file.
pub struct FileIterator<'a, D> where D: 'a {
	/// Number of bytes read from file.
	read: usize,
	/// Data
	data: &'a D,
}

impl<'a, D> FileIterator<'a, D> {
	/// Advance file by n blooms
	pub fn advance(&mut self, n: usize) {
		self.read += n * 256;
	}
}

impl<'a, D> Iterator for FileIterator<'a, D> where D: AsRef<[u8]> {
	type Item = ethbloom::BloomRef<'a>;

	fn next(&mut self) -> Option<Self::Item> {
		let new_end = self.read + 256;
		let data = self.data.as_ref();
		if data.len() < new_end {
			return None;
		}

		let result = array_ref!(data, self.read, 256);
		self.read = new_end;
		Some(result.into())
	}
}

