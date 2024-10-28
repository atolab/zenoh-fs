use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::time::Duration;

pub const FS_EVT_DELAY: u64 = 1;
pub const SANITIZER_PERIOD: Duration = Duration::from_secs(3);
pub const GAP_DOWNLOAD_SCHEDULE: usize = 32;
pub const STUCK_CYCLES_RESET: usize = 3;
pub const MAX_ACCELERATION: usize = 33;

pub const ZFS_BASE_DIR: &str = "zfs";
pub const ZFS_DIGEST: &str = "zfs-digest";
pub const DOWNLOAD_SUBDIR: &str = "download";
pub const UPLOAD_SUBDIR: &str = "upload";
pub const FRAGS_SUBDIR: &str = "frags";
pub const DIGEST_SUBDIR: &str = "digest";
pub const FRAGMENT_SIZE: usize = 32 * 1024;

///
/// The ZFS structure is as follows:
///
/// .zfsd
///   +- digest
///   |    +- download
///   |    +- upload
///   |
///   +- frags
///        +- download
///        +- upload
///
/// The structure used on the Zenoh filesystem storage is the following:
///
///     zfs
///      +- some
///           +- key
///                +- zfs-digest
///                +- 0
///                +- 1
///                +- ..
///                +- n
///
///
/// Where zfs is just the top level directory under the Zenoh File System backend.
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

pub fn zfsd_home() -> String {
    if let Ok(path) = std::env::var("ZFSD_HOME") {
        path
    } else {
        format!("{}/{}", std::env::var("HOME").unwrap(), ".zfsd")
    }
}

// ZFS key-related functions
pub fn zfs_key(key: &str) -> String {
    format!("{}/{}", ZFS_BASE_DIR, key)
}
pub fn zfs_frags_digest_for_key(key: &str) -> String {
    format!("{}/{}/{}", ZFS_BASE_DIR, key, ZFS_DIGEST)
}
pub fn zfs_nth_frag_key(key: &str, n: u32) -> String {
    format!("{}/{}/{}", ZFS_BASE_DIR, key, n)
}

// ZFSD path-related functions
pub fn zfsd_upload_digest_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), DIGEST_SUBDIR, UPLOAD_SUBDIR)
}
pub fn zfsd_download_digest_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), DIGEST_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfsd_upload_frags_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), FRAGS_SUBDIR, UPLOAD_SUBDIR)
}
// These shouldn't be needed any more:

// pub fn zfs_upload_frags_key_prefix() -> String {
//     format!("zfs/{}/{}", FRAGS_SUBDIR, UPLOAD_SUBDIR)
// }
// pub fn zfs_upload_frags_digest_key(key: &str) -> String {
//     format!("zfs/{}/{}/{}/{}", FRAGS_SUBDIR, UPLOAD_SUBDIR, key, ZFS_DIGEST)
// }



pub fn zfsd_download_frags_dir() -> String {
    format!("{}/{}/{}", zfsd_home(), FRAGS_SUBDIR, DOWNLOAD_SUBDIR)
}

pub fn zfsd_download_frags_dir_for_key(k: &str) -> String {
    format!("{}/{}", zfsd_download_frags_dir(), k)
}


pub fn zfsd_upload_frags_dir_for_key(k: &str) -> String {
    format!("{}/{}", zfsd_upload_frags_dir(), k)
}

pub fn zfsd_upload_frag_dir_to_key(path: &str) -> Option<String> {
    path.strip_prefix(&zfsd_upload_frags_dir())
        .map(|s| s[1..].to_string()) // skip the initial "/"
}

pub async fn zfs_read_download_digest_from(
    path: &std::path::Path,
) -> Result<DownloadDigest, String> {
    tokio::fs::read(path)
        .await
        .map_err(zfs_err2str)
        .and_then(|bs| serde_json::from_slice::<crate::DownloadDigest>(&bs).map_err(zfs_err2str))
}
