/// This file implements a file API utilized primarily by the disk manager
use std::fs::File;
use std::io::SeekFrom;

use crate::shared::PAGE_SIZE;

/// Used to write a buffer to a specified offset in the file handle passed in
pub fn write_bytes(mut handle: &File, bytes: &[u8; PAGE_SIZE], offset: u64) -> std::io::Result<()> {
    use std::io::prelude::*;
    handle.seek(SeekFrom::Start(offset))?;
    handle.write(bytes)?;
    Ok(())
}

/// Used to read from a specified offset, enough bytes to fill the passed in buffer
pub fn read_bytes(
    mut handle: &File,
    buffer: &mut [u8; PAGE_SIZE],
    offset: u64,
) -> std::io::Result<()> {
    use std::io::prelude::*;
    handle.seek(SeekFrom::Start(offset))?;
    handle.read(buffer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs::{File, OpenOptions};

    use rayon::ThreadPoolBuilder;

    use super::*;
    use crate::shared::{cwd, Song};
    use crate::storage::buffer::io;
    use crate::sync::{BinarySemaphore, BinarySemaphoreMethods as _, Latch as _, Synchronized};
    use std::sync::Arc;

    struct Context {
        sem: BinarySemaphore,
        handle: File,
        last_written_id: i32,
        num_writes: usize,
    }

    type Ctx = Synchronized<Context>;

    trait FsCtx {
        fn create(handle: File) -> Self;
        fn wait(&self);
    }

    impl FsCtx for Ctx {
        fn create(handle: File) -> Self {
            Synchronized::init(Context {
                sem: BinarySemaphore::init(false),
                handle,
                last_written_id: 0,
                num_writes: 1,
            })
        }

        fn wait(&self) {
            let inner = unsafe { &(*self.data_ptr()) };
            inner.sem.wait();
        }
    }

    fn setup() -> std::io::Result<File> {
        let dir = cwd() + "/tests/fs_tests";
        std::fs::create_dir_all(std::path::Path::new(&dir))?;
        let handle = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(std::path::Path::new(
                &(cwd() + "/tests/fs_tests/test_file.bin"),
            ))?;

        let car_radio = Song::new(0, "Car Radio", "Twenty-One Pilots");
        let buf = io::to_buffer(&car_radio).unwrap();
        write_bytes(&handle, &buf, 0)?;
        Ok(handle)
    }

    fn cleanup() -> std::io::Result<()> {
        let dir = cwd() + "/tests/fs_tests";
        std::fs::remove_dir_all(std::path::Path::new(&dir))?;
        Ok(())
    }

    fn write_song(ctx: &Ctx, song: Song) -> std::io::Result<()> {
        ctx.latch();
        let inner = unsafe { &mut *ctx.data_ptr() };
        if inner.num_writes >= 10 {
            inner.sem.post();
            return Ok(());
        }
        let handle = &inner.handle;
        let buf = io::to_buffer(song).unwrap();
        write_bytes(&handle, &buf, song.id as u64 * PAGE_SIZE as u64)?;
        inner.last_written_id = song.id;
        inner.num_writes += 1;
        ctx.unlatch();
        Ok(())
    }

    fn read_song(ctx: &Ctx) -> std::io::Result<()> {
        ctx.latch();
        let inner = unsafe { &*ctx.data_ptr() };
        let handle = &inner.handle;
        let ctx_last_written_id = unsafe { &(*ctx.data_ptr()).last_written_id };
        let mut buf = [0u8; PAGE_SIZE];
        read_bytes(
            &handle,
            &mut buf,
            *ctx_last_written_id as u64 * PAGE_SIZE as u64,
        )?;
        let decoded: Song = io::from_buffer(&buf).unwrap();
        assert!(decoded.id == *ctx_last_written_id);
        ctx.unlatch();
        Ok(())
    }

    #[test]
    fn test_concurrent_file_io() {
        let setup_result = setup();
        assert!(!setup_result.is_err());
        let handle = setup_result.unwrap();
        let pool = ThreadPoolBuilder::new().num_threads(20).build().unwrap();

        let ctx = Ctx::create(handle);

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

        let num_songs = songs.len();

        for i in 0..num_songs {
            let ctx = ctx.clone();
            let songs = songs.clone();
            pool.spawn(move || {
                assert!(!write_song(&ctx, songs[i]).is_err());
            })
        }

        for _ in 0..5 {
            let ctx = ctx.clone();
            pool.spawn(move || {
                assert!(!read_song(&ctx).is_err());
            })
        }

        ctx.wait();

        let cleanup_result = cleanup();
        assert!(!cleanup_result.is_err());
    }
}
