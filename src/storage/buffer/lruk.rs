use crate::{shared::FrameId, sync::Synchronized};

pub struct LRUKReplacerInternal {
    num_frames: usize,
    k: usize,
}

pub type LRUKReplacer = Synchronized<LRUKReplacerInternal>;

pub trait Replacer {
    fn evict(frame_id: FrameId) -> bool;
    fn record_access(frame_id: FrameId);
}
