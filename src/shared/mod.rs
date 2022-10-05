#![allow(unused)]
use serde::Serialize;
use std::{env, fmt::Display};

pub type FrameId = isize;
pub type PageId = isize;
pub type Oid = u16;

pub const HEADER_ID: usize = 0;
pub const PAGE_SIZE: usize = 4096;
pub const BUFFER_POOL_SIZE: usize = 50;
pub const INVALID_FRAME_ID: isize = -1;
pub const INVALID_PAGE_ID: isize = -1;

pub fn cwd() -> String {
    String::from(env::current_dir().unwrap().to_str().unwrap())
}

use derivative::Derivative;
use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Serialize, Deserialize, Derivative, Clone, Copy)]
#[derivative(Default)]
pub struct Song {
    // the default id value (-1) denotes that the struct is invalid. all valid songs must have a positive id
    #[derivative(Default(value = "-1"))]
    pub id: i32,
    #[derivative(Default(value = "[0u8; 50]"))]
    #[serde_as(as = "[_; 50]")]
    pub title: [u8; 50],
    #[derivative(Default(value = "[0u8; 50]"))]
    #[serde_as(as = "[_; 50]")]
    pub artist: [u8; 50],
}

impl Song {
    pub fn new<'a>(id: i32, title: &'a str, artist: &'a str) -> Song {
        let mut song_buf = [0u8; 50];
        song_buf[..title.len()].copy_from_slice(title.as_bytes());
        let mut artist_buf = [0u8; 50];
        artist_buf[..artist.len()].copy_from_slice(artist.as_bytes());
        return Song {
            id,
            title: song_buf,
            artist: artist_buf,
        };
    }
}

impl Display for Song {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Song [id={} title={} artist={}]",
            self.id,
            String::from_utf8_lossy(&self.title),
            String::from_utf8_lossy(&self.artist)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn path_to_dir() {
        assert_eq!(
            cwd(),
            "/Users/anishsinha/Home/personal/research/symmetric-concurrent/symmetric-concurrent-v4"
        );
    }
}
