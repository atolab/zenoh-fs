use crate::*;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use zenoh::net::Session;

async fn cleanup_download(digest: &DownloadDigest, download_manifest: &str) -> Result<(), String> {
    // Check first if the file has been really created
    let target = std::path::Path::new(&digest.path);
    if target.exists() {
        //TODO: Rust async does not provide ways to lock files. Thus at the present stage
        // we just schedule the delete after checking if the size of the file is not changing.
        let size = target.metadata().unwrap().len();
        async_std::task::sleep(Duration::from_secs(2 * FS_EVT_DELAY)).await;
        if target.metadata().unwrap().len() == size {
            let frags_path = zfs_download_frags_dir_for_key(&digest.key);
            let _ignore = std::fs::remove_dir_all(&frags_path);
            let _ignore = std::fs::remove_file(std::path::Path::new(download_manifest));
        } else {
            log::debug!(
                "The target {} is still being reassembled, clean up will be scheduled later",
                &digest.path
            );
        }
    }
    Ok(())
}

async fn compute_download_gaps(digest: &DownloadDigest) -> Result<BTreeSet<usize>, String> {
    let frags_path = zfs_download_frags_dir_for_key(&digest.key);
    let frag_digest_path = format!("{}/{}", frags_path, ZFS_DIGEST);
    let defrag_digest = read_defrag_digest(&frag_digest_path).await.unwrap();
    let mut frag_set = BTreeSet::new();

    for i in 0..defrag_digest.fragments {
        frag_set.insert(i as usize);
    }
    let path = std::path::Path::new(&frags_path);
    if let Ok(entries) = path.read_dir() {
        for e in entries {
            if let Ok(entry) = e {
                let name = entry
                    .path()
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap()
                    .to_string();
                if let Ok(n) = name.parse() {
                    frag_set.remove(&n);
                }
            }
        }
    }
    Ok(frag_set)
}

pub async fn download_sanitizer(z: Arc<Session>) {
    let mut registry = HashMap::<String, SanitizerRegistryEntry>::new();
    let d3 = zfs_download_digest_dir();
    let dpath = std::path::Path::new(&d3);
    loop {
        async_std::task::sleep(SANITIZER_PERIOD).await;
        if let Ok(entries) = dpath.read_dir() {
            for e in entries {
                if let Ok(entry) = e {
                    match registry.get_mut(entry.path().to_str().unwrap()) {
                        Some(reg_entry) => {
                            let mut gaps: Vec<usize> = compute_download_gaps(&reg_entry.digest)
                                .await
                                .unwrap()
                                .into_iter()
                                .collect();
                            if gaps.len() == 0 {
                                cleanup_download(&reg_entry.digest, entry.path().to_str().unwrap())
                                    .await
                                    .unwrap();
                            } else {
                                gaps.sort();
                                let new_gap_num = gaps.len();
                                let filtered_gaps: Vec<usize> = gaps
                                    .clone()
                                    .into_iter()
                                    .filter(|n| *n >= reg_entry.tide_level)
                                    .collect();

                                let delta = reg_entry.gap_nun - new_gap_num;
                                if delta > 0 {
                                    reg_entry.tide_level = *filtered_gaps.get(0).unwrap();
                                    reg_entry.gap_nun = new_gap_num;
                                    reg_entry.stuck_cycles = 0;
                                } else if reg_entry.stuck_cycles > 0 {
                                    if reg_entry.stuck_cycles == STUCK_CYCLES_RESET {
                                        log::warn!(
                                            "Gaps recovery for {:?} seems to have stalled, this may be due to process restart of disconnections. Restarting fragment sanitiser.",
                                            &reg_entry.digest.key);
                                        reg_entry.tide_level = 0;
                                        let n = std::cmp::min(gaps.len(), GAP_DOWNLOAD_SCHEDULE);
                                        for i in 0..n {
                                            reg_entry.tide_level = *gaps.get(i).unwrap();
                                            async_std::task::spawn(download_fragment(
                                                z.clone(),
                                                reg_entry.digest.key.clone(),
                                                reg_entry.tide_level as u32,
                                            ));
                                        }
                                    } else {
                                        reg_entry.stuck_cycles += 1;
                                        log::warn!(
                                            "Gaps recovery for {:?} is unusually slow -- no progress for the past {} sanitiser cycles",
                                            &reg_entry.digest.key, reg_entry.stuck_cycles
                                        );
                                    }
                                }
                            }
                        }
                        None => {
                            let digest = zfs_read_download_digest_from(entry.path().as_path())
                                .await
                                .unwrap();
                            let mut gaps: Vec<usize> = compute_download_gaps(&digest)
                                .await
                                .unwrap()
                                .into_iter()
                                .collect();
                            gaps.sort();

                            if gaps.len() > 0 {
                                let tide_level = *gaps.get(0).unwrap();
                                let gap_nun = gaps.len();
                                let sre = SanitizerRegistryEntry {
                                    digest: Arc::new(digest),
                                    tide_level,
                                    gap_nun,
                                    stuck_cycles: 0,
                                };
                                registry.insert(entry.path().to_str().unwrap().into(), sre);
                            } else {
                                cleanup_download(&digest, entry.path().to_str().unwrap())
                                    .await
                                    .unwrap();
                            }
                        }
                    }
                }
            }
        } else {
            log::warn!(target: "zfsd", "Sanitizer unable to list the directory {:?}", dpath);
        }
    }
}
pub async fn upload_sanitizer() {
    loop {
        async_std::task::sleep(Duration::from_secs(5)).await;
        // async_std::fs::read_dir()
    }
}
