use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::slice;

use sqlite_vfs::{OpenOptions, Vfs};

pub struct PagesVfs<const PAGE_SIZE: usize>;

struct Page<const PAGE_SIZE: usize> {
    data: [u8; PAGE_SIZE],
    dirty: bool,
}

pub struct Pages<const PAGE_SIZE: usize> {
    count: usize,
    offset: usize,
    pages: HashMap<u32, Page<PAGE_SIZE>>,
}

impl<const PAGE_SIZE: usize> Vfs for PagesVfs<PAGE_SIZE> {
    type File = Pages<PAGE_SIZE>;

    fn open(
        &self,
        _path: &std::path::Path,
        _opts: OpenOptions,
    ) -> Result<Self::File, std::io::Error> {
        // TODO: open file based on path

        let mut pages = Pages {
            count: 0,
            offset: 0,
            pages: Default::default(),
        };

        if let Some(page) = Self::File::get_page(0) {
            // TODO: unwrap?
            pages.count = u32::from_be_bytes(page[28..32].try_into().unwrap()) as usize;
        }

        Ok(pages)
    }

    fn delete(&self, _path: &std::path::Path) -> Result<(), std::io::Error> {
        // Only used to delete journal or wal files, which both are not implemented yet, thus simply
        // ignored for now.
        Ok(())
    }

    fn exists(&self, _path: &Path) -> Result<bool, std::io::Error> {
        // Only used to check existance of journal or wal files, which both are not implemented yet,
        // thus simply always return `false` for now.
        Ok(false)
    }
}

impl<const PAGE_SIZE: usize> sqlite_vfs::File for Pages<PAGE_SIZE> {
    fn file_size(&self) -> Result<u64, std::io::Error> {
        Ok((self.count * PAGE_SIZE) as u64)
    }

    fn truncate(&mut self, _size: u64) -> Result<(), std::io::Error> {
        unimplemented!("truncate")
    }
}

impl<const PAGE_SIZE: usize> Seek for Pages<PAGE_SIZE> {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let offset = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::End(_) => unimplemented!(),
            SeekFrom::Current(_) => unimplemented!(),
        };

        self.offset = offset as usize;

        Ok(self.offset as u64)
    }
}

impl<const PAGE_SIZE: usize> Read for Pages<PAGE_SIZE> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.offset % PAGE_SIZE;
        let page = self.current()?;
        let n = (&page.data[offset..]).read(buf)?;
        self.offset += n;
        Ok(n)
    }
}

impl<const PAGE_SIZE: usize> Write for Pages<PAGE_SIZE> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let offset = self.offset % PAGE_SIZE;
        let page = self.current()?;
        let n = (&mut page.data[offset..]).write(buf)?;
        page.dirty = true;
        self.offset += n;

        let count = (self.offset / PAGE_SIZE) + 1;
        if count > self.count {
            self.count = count;
        }

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        for (index, page) in &mut self.pages {
            if page.dirty {
                Self::put_page(*index, &page.data);
                page.dirty = false;
            }
        }
        Ok(())
    }
}

impl<const PAGE_SIZE: usize> Pages<PAGE_SIZE> {
    fn current(&mut self) -> Result<&mut Page<PAGE_SIZE>, std::io::Error> {
        let index = self.offset / PAGE_SIZE;

        if let Entry::Vacant(entry) = self.pages.entry(index as u32) {
            let data = Self::get_page(index as u32);
            entry.insert(Page {
                data: data.unwrap_or_else(|| [0; PAGE_SIZE]),
                dirty: false,
            });
        }

        Ok(self.pages.get_mut(&(index as u32)).unwrap())
    }

    pub fn get_page(ix: u32) -> Option<[u8; PAGE_SIZE]> {
        unsafe {
            let ptr = crate::get_page(ix);
            if ptr.is_null() {
                None
            } else {
                let slice = slice::from_raw_parts_mut(ptr, PAGE_SIZE);
                slice[..].try_into().ok()
            }
        }
    }

    fn put_page(ix: u32, data: &[u8; PAGE_SIZE]) {
        unsafe {
            crate::put_page(ix, data.as_ptr());
        }
    }
}
