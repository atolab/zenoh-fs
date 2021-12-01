use std::time::Duration;
use serde::{Deserialize, Serialize};

pub static  ZFS_DIGEST: &str = "zfs-digest";
pub static FRAGMENT_SIZE: usize = 1024;

#[derive(Debug, Serialize, Deserialize)]
pub struct FragmentationDigest {
    pub name: String,    
    pub crc: u64,
    pub fragment_size: usize,
    pub fragments: u32
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadDigest {
    pub path: String,
    pub key: String
} 

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadDigest {
    pub key: String,
    pub path: String,
    pub pace: Duration,
    
} 

pub mod frag;
