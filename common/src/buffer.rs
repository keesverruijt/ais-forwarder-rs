use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::IoSlice;
use std::io::IoSliceMut;
use std::io::Read;
use std::io::Result;
use std::io::Write;
use std::ops::Deref;

pub struct BufReaderDirectWriter<T: ?Sized> {
    pub(crate) inner: BufReader<T>,
}

impl<T: Read> BufReaderDirectWriter<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: BufReader::new(inner),
        }
    }
}

impl<T: Read + ?Sized> Read for BufReaderDirectWriter<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        self.inner.read_vectored(bufs)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.read_exact(buf)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.inner.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        self.inner.read_to_string(buf)
    }
}

impl<T: Read + ?Sized> BufRead for BufReaderDirectWriter<T> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        self.inner.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt)
    }
}

impl<T: Write + ?Sized> Write for BufReaderDirectWriter<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.get_mut().write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.get_mut().write_all(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.inner.get_mut().write_vectored(bufs)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.get_mut().flush()
    }
}

impl<T> Deref for BufReaderDirectWriter<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner.get_ref()
    }
}

impl<T: PartialEq> PartialEq for BufReaderDirectWriter<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.get_ref() == other.inner.get_ref()
    }
}

pub struct BufWriterDirectReader<T: ?Sized + Write> {
    pub(crate) inner: BufWriter<T>,
}

impl<T: Write> BufWriterDirectReader<T> {
    #[allow(dead_code)]
    pub fn new(inner: T) -> Self {
        Self {
            inner: BufWriter::new(inner),
        }
    }
}

impl<T: Write + ?Sized> Write for BufWriterDirectReader<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.inner.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.inner.write_all(buf)
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.inner.write_vectored(bufs)
    }

    fn flush(&mut self) -> Result<()> {
        self.inner.flush()
    }
}

impl<T: Write + Read + ?Sized> Read for BufWriterDirectReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.get_mut().read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        self.inner.get_mut().read_vectored(bufs)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.inner.get_mut().read_exact(buf)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        self.inner.get_mut().read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        self.inner.get_mut().read_to_string(buf)
    }
}

impl<T: Write + BufRead + ?Sized> BufRead for BufWriterDirectReader<T> {
    fn fill_buf(&mut self) -> Result<&[u8]> {
        self.inner.get_mut().fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.inner.get_mut().consume(amt)
    }
}

impl<T: PartialEq + Write> PartialEq for BufWriterDirectReader<T> {
    fn eq(&self, other: &Self) -> bool {
        self.inner.get_ref() == other.inner.get_ref()
    }
}
