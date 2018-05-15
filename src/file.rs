use std::io::{Seek, SeekFrom, Write, Read};
use std::path::Path;
use std::{io, fs};
use {ethbloom};

/// Autoresizable file containing blooms.
pub struct File {
	/// Backing file.
	file: fs::File,
	/// Current file len.
	len: u64,
}

impl File {
	/// Opens database file. Creates new file if database file does not exist.
	pub fn open<P>(path: P) -> io::Result<File> where P: AsRef<Path> {
		let file = fs::OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.append(true)
			.open(path)?;
		let len = file.metadata()?.len();

		let file = File {
			file,
			len,
		};

		Ok(file)

	}

	/// Resizes the file if there is not enough space to write bloom at given position.
	fn ensure_space_for_write(&mut self, pos: u64) -> io::Result<()> {
		// position to write + 256 bytes
		let required_space = (pos + 1) * 256;
		if required_space > self.len {
			self.file.set_len(required_space)?;
			self.len = required_space;
		}
		Ok(())
	}

	/// Read bloom at given position.
	pub fn read_bloom(&self, pos: u64) -> io::Result<ethbloom::Bloom> {
		let mut file_ref = &self.file;
		file_ref.seek(SeekFrom::Start(pos * 256))?;
		let mut bloom = ethbloom::Bloom::default();
		file_ref.read_exact(&mut bloom)?;
		Ok(bloom)
	}

	/// Accrue bloom into bloom at given position.
	pub fn accrue_bloom<'a, B>(&mut self, pos: u64, bloom: B) -> io::Result<()> where ethbloom::BloomRef<'a>: From<B> {
		self.ensure_space_for_write(pos)?;
		let mut old_bloom: ethbloom::Bloom = self.read_bloom(pos)?;
		old_bloom.accrue_bloom(bloom);
		let mut file_ref = &self.file;
		file_ref.seek(SeekFrom::Start(pos * 256))?;
		file_ref.write_all(&old_bloom)
	}

	/// Replace bloom at given position with a new one.
	pub fn replace_bloom<'a, B>(&mut self, pos: u64, bloom: B) -> io::Result<()> where ethbloom::BloomRef<'a>: From<B> {
		self.ensure_space_for_write(pos)?;
		let mut file_ref = &self.file;
		file_ref.seek(SeekFrom::Start(pos * 256))?;
		file_ref.write_all(ethbloom::BloomRef::from(bloom).data())
	}

	/// Returns an iterator over file.
	pub fn iterator(&self) -> io::Result<FileIterator> {
		let mut file_ref = &self.file;
		file_ref.seek(SeekFrom::Start(0))?;

		let iter = FileIterator {
			file: file_ref,
		};

		Ok(iter)
	}

	/// Flush outstanding modifications to the disk
	pub fn flush(&mut self) -> io::Result<()> {
		self.file.flush()
	}
}

/// Iterator over blooms of a single file.
pub struct FileIterator<'a> {
	/// Backing file.
	file: &'a fs::File,
}

impl<'a> FileIterator<'a> {
	/// Advance file by n blooms
	pub fn advance(&mut self, n: u64) -> io::Result<()> {
		self.file.seek(SeekFrom::Current(n as i64 * 256))?;
		Ok(())
	}
}

impl<'a> Iterator for FileIterator<'a> {
	type Item = io::Result<ethbloom::Bloom>;

	fn next(&mut self) -> Option<Self::Item> {
		let mut bloom = ethbloom::Bloom::default();
		match self.file.read_exact(&mut bloom) {
			Ok(_) => Some(Ok(bloom)),
			Err(ref err) if err.kind() == io::ErrorKind::UnexpectedEof => None,
			Err(err) => Some(Err(err)),
		}
	}
}

