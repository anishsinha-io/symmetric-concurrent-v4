#![allow(unused)]

use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};

use crate::shared::{FrameId, PageId, BUFFER_POOL_SIZE, PAGE_SIZE};
use crate::storage::buffer::diskmgr::{DiskApi as _, DiskMgr};
use crate::storage::buffer::page;
use crate::storage::buffer::page::Page;
use crate::sync::hashtable::HashTable;
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
    page_table: HashTable<PageId, FrameId>,
}

pub trait BufApi {
    fn create(path: &str) -> Self;
    fn size(&self) -> usize;
    fn new_page(&self, page_id: PageId) -> Option<Page>;
    fn fetch_page(&self, page_id: PageId) -> Page;
    fn unpin_page(&self, page_id: PageId);
    fn flush_page(&self, page_id: PageId) -> bool;
    fn flush_all(&self);
    fn delete_page(&self, page_id: PageId) -> bool;
    fn alloc_page(&self) -> PageId;
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

    ///
    /// TODO Add implementation
    ///
    /// @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
    /// are currently in use and not evictable (in another word, pinned).
    ///
    /// You should pick the replacement frame from either the free list or the replacer (always find from the free list
    /// first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
    /// you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
    ///
    /// Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
    /// so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
    /// Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
    ///
    /// @param[out] page_id id of created page
    /// @return nullptr if no new pages could be created, otherwise pointer to new page
    ///
    fn new_page(&self, page_id: PageId) -> Option<Page> {
        todo!()
    }

    fn fetch_page(&self, page_id: PageId) -> Page {
        todo!();
    }

    fn unpin_page(&self, page_id: PageId) {
        todo!();
    }

    fn flush_page(&self, page_id: PageId) -> bool {
        todo!();
    }

    fn flush_all(&self) {
        todo!();
    }

    fn delete_page(&self, page_id: PageId) -> bool {
        todo!();
    }

    fn alloc_page(&self) -> PageId {
        let inner = self.read();
        let buf = page::empty();
        let page_id = inner.mgr.append_page(&buf).unwrap();
        page_id
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
        let y = lst.pop_back().unwrap();
        assert!(x == 1);
        assert!(y == 50);
    }
}
