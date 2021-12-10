use serde::{Deserialize, Serialize};

pub static ZFS_DIGEST: &str = "zfs-digest";
pub const EVT_DELAY: u64 = 1;
pub const DOWNLOAD_SUBDIR: &str = "download";
pub const UPLOAD_SUBDIR: &str = "upload";
pub const FRAGS_SUBDIR: &str = "frags";
pub const FRAGMENT_SIZE: usize = 4 * 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct FragmentationDigest {
    pub name: String,
    pub crc: u64,
    pub fragment_size: usize,
    pub fragments: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadDigest {
    pub path: String,
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadDigest {
    pub key: String,
    pub path: String,
    pub pace: u32,
}

mod frag;
mod transfer;

pub use frag::*;
pub use transfer::*;

pub fn zfs_home() -> String {
    if let Ok(zfs_home) = std::env::var("ZFS_HOME") {
        zfs_home
    } else {
        format!("{}/{}", std::env::var("HOME").unwrap(), ".zfs")
    }
}