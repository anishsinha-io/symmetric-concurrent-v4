#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};

use crate::shared::{FrameId, PageId, BUFFER_POOL_SIZE, PAGE_SIZE};
use crate::storage::buffer::diskmgr::{DiskApi as _, DiskMgr};
use crate::storage::buffer::page::Page;
use crate::sync::{Latch as _, RwLatch as _, RwSynchronized, Synchronized};

pub struct BufferPoolFrameInternal {
    page: Page,
    id: FrameId,
    pin_count: usize,
    dirty: bool,
}

pub trait FrameApi {
    fn data(&self) -> Page;
    fn is_dirty(&self) -> bool;
    fn reset(&self);
}

pub type BufferPoolFrame = RwSynchronized<BufferPoolFrameInternal>;

/// Latching functions taken care of by RwLatch implementation
impl FrameApi for BufferPoolFrame {
    fn data(&self) -> Page {
        let inner = unsafe { &*self.data_ptr() };
        inner.page
    }

    fn is_dirty(&self) -> bool {
        let inner = unsafe { &*self.data_ptr() };
        inner.dirty
    }

    fn reset(&self) {
        let mut inner = unsafe { &mut *self.data_ptr() };
        inner.page = [0u8; PAGE_SIZE];
    }
}

pub struct BufferPoolContext {
    mgr: DiskMgr,
    frames: Vec<RwSynchronized<BufferPoolFrameInternal>>,
    free_list: RefCell<LinkedList<FrameId>>,
    page_table: Synchronized<HashMap<PageId, FrameId>>,
}

pub trait BufApi {
    fn create(path: &str) -> Self;
    fn size(&self) -> usize;
}

pub type BufferPool = RwSynchronized<BufferPoolContext>;

impl BufApi for BufferPool {
    fn create(path: &str) -> Self {
        let mut free_list: LinkedList<FrameId> = LinkedList::new();
        for i in 1..BUFFER_POOL_SIZE + 1 {
            free_list.push_back(i as FrameId);
        }
        RwSynchronized::init(BufferPoolContext {
            mgr: DiskMgr::create(&path),
            frames: Vec::new(),
            free_list: RefCell::new(free_list),
            page_table: Synchronized::init(HashMap::new()),
        })
    }

    #[inline]
    fn size(&self) -> usize {
        let inner = self.read();
        inner.frames.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::shared::cwd;

    #[test]
    fn test_create() {
        let dir = cwd() + "/tests/bufmgr_tests";
        std::fs::create_dir_all(std::path::Path::new(&dir)).unwrap();

        let path = cwd() + "/tests/bufmgr_tests/test_create_file.bin";
        let buffer_pool = BufferPool::create(&path);

        let inner = buffer_pool.read();
        let lst = &mut inner.free_list.clone().into_inner();
        assert!(lst.len() == BUFFER_POOL_SIZE);
        assert!(inner.frames.len() == 0);

        let x = lst.pop_front().unwrap();
        assert!(x == 1);
    }
}
