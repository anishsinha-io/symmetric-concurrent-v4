use crate::shared::PAGE_SIZE;

pub type Page = [u8; PAGE_SIZE];

#[inline]
pub fn empty() -> Page {
    [0u8; PAGE_SIZE]
}
