use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sqlite_vfs::{LockKind, OpenKind, OpenOptions, Vfs};

#[derive(Default)]
pub struct PagesVfs<const PAGE_SIZE: usize> {
    lock_state: Arc<Mutex<LockState>>,
}

#[derive(Debug, Default)]
struct LockState {
    read: usize,
    write: Option<bool>,
}

pub struct Connection<const PAGE_SIZE: usize> {
    lock_state: Arc<Mutex<LockState>>,
    lock: LockKind,
}

impl<const PAGE_SIZE: usize> Vfs for PagesVfs<PAGE_SIZE> {
    type Handle = Connection<PAGE_SIZE>;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error> {
        // Always open the same database for now.
        if db != "main.db" {
            return Err(io::Error::new(
                ErrorKind::NotFound,
                format!("unexpected database name `{}`; expected `main.db3`", db),
            ));
        }

        // Only main databases supported right now (no journal, wal, temporary, ...)
        if opts.kind != OpenKind::MainDb {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "only main database supported right now (no journal, wal, ...)",
            ));
        }

        Ok(Connection {
            lock_state: self.lock_state.clone(),
            lock: LockKind::None,
        })
    }

    fn delete(&self, _db: &str) -> Result<(), std::io::Error> {
        // Only used to delete journal or wal files, which both are not implemented yet, thus simply
        // ignored for now.
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        Ok(db == "main.db" && Connection::<PAGE_SIZE>::page_count() > 0)
    }

    fn temporary_name(&self) -> String {
        String::from("main.db")
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::Rng::fill(&mut rand::thread_rng(), buffer);
    }

    fn sleep(&self, duration: Duration) -> Duration {
        let now = Instant::now();
        unsafe { crate::conn_sleep((duration.as_millis() as u32).max(1)) };
        now.elapsed()
    }
}

impl<const PAGE_SIZE: usize> sqlite_vfs::DatabaseHandle for Connection<PAGE_SIZE> {
    type WalIndex = sqlite_vfs::WalDisabled;

    fn size(&self) -> Result<u64, io::Error> {
        let size = Self::page_count() * PAGE_SIZE;
        eprintln!("size={}", size);
        Ok(size as u64)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        let index = offset as usize / PAGE_SIZE;
        let offset = offset as usize % PAGE_SIZE;

        let data = Self::get_page(index as u32);
        if data.len() < buf.len() + offset {
            eprintln!(
                "read {} < {} -> UnexpectedEof",
                data.len(),
                buf.len() + offset
            );
            return Err(ErrorKind::UnexpectedEof.into());
        }

        eprintln!("read index={} len={} offset={}", index, buf.len(), offset);
        buf.copy_from_slice(&data[offset..offset + buf.len()]);

        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if offset as usize % PAGE_SIZE > 0 {
            return Err(io::Error::new(
                ErrorKind::Other,
                "unexpected write across page boundaries",
            ));
        }

        let index = offset as usize / PAGE_SIZE;
        let page = buf.try_into().map_err(|_| {
            io::Error::new(
                ErrorKind::Other,
                format!(
                    "unexpected write size {}; expected {}",
                    buf.len(),
                    PAGE_SIZE
                ),
            )
        })?;
        eprintln!("write index={} len={}", index, buf.len());
        Self::put_page(index as u32, page);

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        // Everything is directly written to storage, so no extra steps necessary to sync.
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        eprintln!("set_len={}", size);

        let mut page_count = size as usize / PAGE_SIZE;
        if size as usize % PAGE_SIZE > 0 {
            page_count += 1;
        }

        let current_page_count = Self::page_count();
        if page_count > 0 && page_count < current_page_count {
            for i in (page_count..current_page_count).into_iter().rev() {
                Self::del_page(i as u32);
            }
        }

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let ok = Self::lock(self, lock);
        // eprintln!("locked = {}", ok);
        Ok(ok)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        Ok(Self::reserved(self))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        Ok(sqlite_vfs::WalDisabled::default())
    }

    fn set_chunk_size(&self, chunk_size: usize) -> Result<(), io::Error> {
        if chunk_size != PAGE_SIZE {
            eprintln!("set_chunk_size={} (rejected)", chunk_size);
            Err(io::Error::new(
                ErrorKind::Other,
                "changing chunk size is not allowed",
            ))
        } else {
            eprintln!("set_chunk_size={}", chunk_size);
            Ok(())
        }
    }
}

impl<const PAGE_SIZE: usize> Connection<PAGE_SIZE> {
    fn get_page(ix: u32) -> [u8; PAGE_SIZE] {
        let mut data = [0u8; PAGE_SIZE];
        unsafe { crate::get_page(ix, data.as_mut_ptr()) };
        data
    }

    fn put_page(ix: u32, data: &[u8; PAGE_SIZE]) {
        unsafe {
            crate::put_page(ix, data.as_ptr());
        }
    }

    fn del_page(ix: u32) {
        unsafe {
            crate::del_page(ix);
        }
    }

    fn page_count() -> usize {
        unsafe { crate::page_count() as usize }
    }

    fn lock(&mut self, to: LockKind) -> bool {
        if self.lock == to {
            return true;
        }

        let mut lock_state = self.lock_state.lock().unwrap();

        // eprintln!(
        //     "lock state={:?} from={:?} to={:?}",
        //     lock_state, self.lock, to
        // );

        // The following locking implementation is probably not sound (wouldn't be surprised if it
        // potentially dead-locks), but suffice for the experiment.

        match to {
            LockKind::None => {
                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                } else if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::None;
                true
            }

            LockKind::Shared => {
                if lock_state.write == Some(true) && self.lock <= LockKind::Shared {
                    return false;
                }

                lock_state.read += 1;
                if self.lock > LockKind::Shared {
                    lock_state.write = None;
                }
                self.lock = LockKind::Shared;
                true
            }

            LockKind::Reserved => {
                if lock_state.write.is_some() || self.lock != LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }
                lock_state.write = Some(false);
                self.lock = LockKind::Reserved;
                true
            }

            LockKind::Pending => {
                // cannot be requested directly
                false
            }

            LockKind::Exclusive => {
                if lock_state.write.is_some() && self.lock <= LockKind::Shared {
                    return false;
                }

                if self.lock == LockKind::Shared {
                    lock_state.read -= 1;
                }

                lock_state.write = Some(true);
                if lock_state.read == 0 {
                    self.lock = LockKind::Exclusive;
                    true
                } else {
                    self.lock = LockKind::Pending;
                    false
                }
            }
        }
    }

    fn reserved(&self) -> bool {
        if self.lock > LockKind::Shared {
            return true;
        }

        let lock_state = self.lock_state.lock().unwrap();
        lock_state.write.is_some()
    }
}

impl<const PAGE_SIZE: usize> Drop for Connection<PAGE_SIZE> {
    fn drop(&mut self) {
        if self.lock != LockKind::None {
            self.lock(LockKind::None);
        }
    }
}
