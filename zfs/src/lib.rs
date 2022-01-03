use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::time::Duration;

pub const FS_EVT_DELAY: u64 = 1;
pub const SANITIZER_PERIOD: Duration = Duration::from_secs(3);
pub const GAP_DOWNLOAD_SCHEDULE: usize = 32;
pub const STUCK_CYCLES_RESET: usize = 3;
pub const MAX_ACCELERATION: usize = 33;

pub static ZFS_DIGEST: &str = "zfs-digest";
pub const DOWNLOAD_SUBDIR: &str = "download";
pub const UPLOAD_SUBDIR: &str = "upload";
pub const FRAGS_SUBDIR: &str = "frags";
pub const DIGEST_SUBDIR: &str = "digest";
pub const FRAGMENT_SIZE: usize = 32 * 1024;

///
/// The ZFS structure is as follows:
/// .zfs
///   +- digest
///   |    +- download
///   |    +- upload
///   |
///   +- frags
///        +- download
///        +- upload
///
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FragmentationDigest {
    pub name: String,
    pub size: u64,
    pub crc: u64,
    pub fragment_size: usize,
    pub fragments: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadDigest {
    pub path: String,
    pub key: String,
    pub fragment_size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadDigest {
    pub key: String,
    pub path: String,
    pub pace: usize,
}

#[derive(Debug)]
struct SanitizerRegistryEntry {
    digest: std::sync::Arc<DownloadDigest>,
    tide_level: usize,
    gap_nun: usize,
    stuck_cycles: usize,
}

mod frag;
mod sanitizer;
mod transfer;

pub use frag::*;
pub use sanitizer::{download_sanitizer, upload_sanitizer};
pub use transfer::*;

pub fn zfs_err2str<E: Debug>(e: E) -> String {
    format!("{:?}", e)
}
pub fn zfs_home() -> String {
    if let Ok(zfs_home) = std::env::var("ZFS_HOME") {
        zfs_home
    } else {
        format!("{}/{}", std::env::var("HOME").unwrap(), ".zfs")
    }
}
pub fn zfs_upload_digest_dir() -> String {
    format!("{}/{}/{}", zfs_home(), DIGEST_SUBDIR, UPLOAD_SUBDIR)
}
pub fn zfs_download_digest_dir() -> String {
    format!("{}/{}/{}", zfs_home(), DIGEST_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfs_upload_frags_dir() -> String {
    format!("{}/{}/{}", zfs_home(), FRAGS_SUBDIR, UPLOAD_SUBDIR)
}

pub fn zfs_download_frags_dir() -> String {
    format!("{}/{}/{}", zfs_home(), FRAGS_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfs_download_frags_dir_for_key(k: &str) -> String {
    k.chars()
        .next()
        .map(|c| {
            if c == '/' {
                format!("{}{}", zfs_download_frags_dir(), k)
            } else {
                format!("{}/{}", zfs_download_frags_dir(), k)
            }
        })
        .unwrap()
}

pub fn zfs_upload_frags_dir_for_key(k: &str) -> String {
    k.chars()
        .next()
        .map(|c| {
            if c == '/' {
                format!("{}{}", zfs_upload_frags_dir(), k)
            } else {
                format!("{}/{}", zfs_upload_frags_dir(), k)
            }
        })
        .unwrap()
}

pub fn zfs_upload_frag_dir_to_key(path: &str) -> Option<String> {
    path.strip_prefix(&zfs_upload_frags_dir())
        .map(|s| s.to_string())
}

pub async fn zfs_read_download_digest_from(
    path: &std::path::Path,
) -> Result<DownloadDigest, String> {
    async_std::fs::read(path)
        .await
        .map_err(zfs_err2str)
        .and_then(|bs| serde_json::from_slice::<crate::DownloadDigest>(&bs).map_err(zfs_err2str))
}
