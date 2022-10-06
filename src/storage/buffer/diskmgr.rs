use std::fs::{File, OpenOptions};

use crate::shared::{PageId, PAGE_SIZE};
use crate::storage::buffer;
use crate::sync::{Latch as _, Synchronized};

pub struct DiskMgrCtx {
    num_writes: usize,
    last_write: isize,
    num_flushes: usize,
    handle: File,
}

pub type DiskMgr = Synchronized<DiskMgrCtx>;

pub trait DiskApi {
    fn create(path: &str) -> Self;
    fn read_page(&self, buf: &mut [u8; PAGE_SIZE], offset: u64) -> std::io::Result<()>;
    fn write_page(&self, buf: &[u8; PAGE_SIZE], offset: u64) -> std::io::Result<()>;
    fn append_page(&self, buf: &[u8; PAGE_SIZE]) -> std::io::Result<PageId>;
    fn inner(&self) -> &mut DiskMgrCtx;
}

impl DiskApi for DiskMgr {
    fn create(path: &str) -> Self {
        let handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(std::path::Path::new(path))
            .unwrap();

        Synchronized::init(DiskMgrCtx {
            handle,
            num_writes: 0,
            num_flushes: 0,
            last_write: -1,
        })
    }

    fn read_page(&self, buf: &mut [u8; PAGE_SIZE], loc: u64) -> std::io::Result<()> {
        let inner = self.inner();
        buffer::fs::read_bytes(&inner.handle, buf, loc * PAGE_SIZE as u64)?;
        Ok(())
    }

    fn write_page(&self, buf: &[u8; PAGE_SIZE], loc: u64) -> std::io::Result<()> {
        let mut inner = self.inner();
        buffer::fs::write_bytes(&inner.handle, buf, loc * PAGE_SIZE as u64)?;
        inner.num_writes += 1;
        inner.handle.sync_all()?;
        inner.num_flushes += 1;
        inner.last_write = loc as isize;
        Ok(())
    }

    fn append_page(&self, buf: &[u8; PAGE_SIZE]) -> std::io::Result<PageId> {
        let mut inner = self.inner();
        let page_id = buffer::fs::append_bytes(&inner.handle, &buf)?;
        inner.num_writes += 1;
        inner.handle.sync_all()?;
        inner.num_flushes += 1;
        inner.last_write = page_id;
        Ok(page_id)
    }

    fn inner(&self) -> &mut DiskMgrCtx {
        unsafe { &mut *self.data_ptr() }
    }
}

#[cfg(test)]
mod tests {
    use rayon::ThreadPoolBuilder;

    use std::sync::Arc;

    use super::*;
    use crate::shared::{cwd, Song};
    use crate::storage::buffer::io;
    use crate::sync::{BinarySemaphore, BinarySemaphoreMethods as _};

    fn setup() -> std::io::Result<String> {
        let dir = cwd() + "/tests/diskmgr_tests";
        std::fs::create_dir_all(std::path::Path::new(&dir))?;
        Ok((cwd() + "/tests/diskmgr_tests/test_file.bin").to_string())
    }

    fn cleanup() -> std::io::Result<()> {
        let dir = cwd() + "/tests/diskmgr_tests";
        std::fs::remove_dir_all(std::path::Path::new(&dir))?;
        Ok(())
    }

    fn write_song(mgr: &DiskMgr, song: &Song, sem: &BinarySemaphore) -> std::io::Result<()> {
        mgr.latch();
        let inner = unsafe { &mut *mgr.data_ptr() };
        if inner.num_writes >= 5 {
            sem.post();
            return Ok(());
        }
        let buf = io::to_buffer(song).unwrap();
        mgr.write_page(&buf, song.id as u64)?;
        println!("written song with id {}", song.id);
        mgr.unlatch();
        Ok(())
    }

    fn read_song(mgr: &DiskMgr) -> std::io::Result<()> {
        mgr.latch();
        let inner = unsafe { &*mgr.data_ptr() };
        let mut buf = [0u8; PAGE_SIZE];
        mgr.read_page(&mut buf, inner.last_write as u64)?;
        let decoded: Song = io::from_buffer(&buf).unwrap();
        println!("last written: {}", inner.last_write);
        println!("read: {}", decoded);
        mgr.unlatch();
        Ok(())
    }

    #[test]
    fn test_concurrent_diskmgr() {
        let setup_result = setup();
        assert!(!setup_result.is_err());
        let path = setup_result.unwrap();
        let pool = ThreadPoolBuilder::new().num_threads(20).build().unwrap();
        let sem = BinarySemaphore::init(false);
        let diskmgr = DiskMgr::create(&path);

        let sweater_weather = Song::new(1, "Sweater Weather", "The Neighbourhood");
        let softcore = Song::new(2, "Softcore", "The Neighbourhood");
        let daddy_issues = Song::new(3, "Daddy Issues", "The Neighbourhood");
        let reflections = Song::new(4, "Reflections", "The Neighbourhood");
        let the_beach = Song::new(5, "The Beach", "The Neighbourhood");
        let afraid = Song::new(6, "Afraid", "The Neighbourhood");
        let cry_baby = Song::new(7, "Cry Baby", "The Neighbourhood");
        let scary_love = Song::new(8, "Scary Love", "The Neighbourhood");
        let nervous = Song::new(9, "Nervous", "The Neighbourhood");
        let stargazing = Song::new(10, "Stargazing", "The Neighbourhood");
        let prey = Song::new(11, "Prey", "The Neighbourhood");
        let compass = Song::new(12, "Compass", "The Neighbourhood");
        let wires = Song::new(13, "Wires", "The Neighbourhood");
        let stuck_with_me = Song::new(14, "Stuck With Me", "The Neighbourhood");
        let flawless = Song::new(15, "Flawless", "The Neighbourhood");

        let songs = Arc::new(vec![
            sweater_weather,
            softcore,
            daddy_issues,
            reflections,
            the_beach,
            afraid,
            cry_baby,
            scary_love,
            nervous,
            stargazing,
            prey,
            compass,
            wires,
            stuck_with_me,
            flawless,
        ]);

        for i in 0..songs.len() / 2 {
            let sem = sem.clone();
            let diskmgr = diskmgr.clone();
            let songs = songs.clone();
            pool.spawn(move || {
                let song = songs[i];
                assert!(!write_song(&diskmgr, &song, &sem).is_err());
            })
        }

        for _ in 0..songs.len() / 2 {
            let diskmgr = diskmgr.clone();
            pool.spawn(move || {
                assert!(!read_song(&diskmgr).is_err());
            })
        }

        let state = sem.wait();
        assert!(state);
        assert!(!cleanup().is_err());
    }
}
