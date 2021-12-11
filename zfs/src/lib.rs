use serde::{Deserialize, Serialize};

pub static ZFS_DIGEST: &str = "zfs-digest";
pub const EVT_DELAY: u64 = 1;
pub const DOWNLOAD_SUBDIR: &str = "download";
pub const UPLOAD_SUBDIR: &str = "upload";
pub const FRAGS_SUBDIR: &str = "frags";
pub const DIGEST_SUBDIR: &str = "digest";
pub const FRAGMENT_SIZE: usize = 4 * 1024;

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
    k.chars().nth(0)
        .and_then(
            |c| if c == '/' { Some(format!("{}{}", zfs_download_frags_dir(), k)) }
                      else { Some(format!("{}/{}", zfs_download_frags_dir(), k)) }
        ).unwrap()
}

pub fn zfs_upload_frags_dir_for_key(k: &str) -> String {
    k.chars().nth(0)
        .and_then(
            |c| if c == '/' { Some(format!("{}{}", zfs_upload_frags_dir(), k)) }
            else { Some(format!("{}/{}", zfs_upload_frags_dir(), k)) }
        ).unwrap()
}

pub fn zfs_upload_frag_dir_to_key(path: &str) -> Option<String> {
    path.strip_prefix(&zfs_upload_frags_dir()).map(|s| s.to_string())
}