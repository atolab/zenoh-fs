use crate::*;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

async fn cleanup_download(digest: &DownloadDigest, download_manifest: &str) -> Result<(), String> {
    // Check first if the file has been really created
    let target = std::path::Path::new(&digest.path);
    let frags_path = zfs_download_frags_dir_for_key(&digest.key);
    let fmanif_exists = std::path::Path::new(&format!("{}/{}", &frags_path, ZFS_DIGEST)).exists();
    if target.exists() && fmanif_exists {
        let defrag_digest = read_defrag_digest(&frags_path).await.unwrap();
        let size = target.metadata().unwrap().len();
        async_std::task::sleep(Duration::from_secs(2 * FS_EVT_DELAY)).await;
        if size == defrag_digest.size {
            let frags_path = zfs_download_frags_dir_for_key(&digest.key);
            let _ignore = std::fs::remove_dir_all(&frags_path);
            let _ignore = std::fs::remove_file(std::path::Path::new(download_manifest));
        } else {
            log::debug!(
                "The target {} is still being reassembled, clean up will be scheduled later {} != {}",&digest.path, size, defrag_digest.size,

            );
        }
    } else if !target.exists() && fmanif_exists {
        // We try to defragment...
        let _ignore = defragment(&digest.key, &digest.path).await;
    }
    Ok(())
}

async fn compute_download_gaps(digest: &DownloadDigest) -> Result<BTreeSet<usize>, String> {
    let frags_path = zfs_download_frags_dir_for_key(&digest.key);
    if let Ok(defrag_digest) = read_defrag_digest(&frags_path).await {
        let fmanif_exists =
            std::path::Path::new(&format!("{}/{}", &frags_path, ZFS_DIGEST)).exists();
        if !fmanif_exists {
            return Err("There is manifest, this gaps cannot be computed".to_string());
        }
        let mut frag_set = BTreeSet::new();
        for i in 0..defrag_digest.fragments {
            frag_set.insert(i as usize);
        }
        let path = std::path::Path::new(&frags_path);
        if let Ok(entries) = path.read_dir() {
            for entry in entries.flatten() {
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
        Ok(frag_set)
    } else {
        Err(format!("Unable to read defrag digest for {:?}", &digest))
    }
}

fn compute_acceleration_factor(stuck_cycles: usize) -> usize {
    let r = (stuck_cycles / STUCK_CYCLES_RESET) + 1;
    let a = std::cmp::max(1, r / 2);
    let f = std::cmp::min(a * r, MAX_ACCELERATION);
    log::debug!("Acceleration factor for {} is {}", stuck_cycles, f);
    f
}
pub async fn download_sanitizer(z: Arc<zenoh::Session>) {
    let mut registry = HashMap::<String, SanitizerRegistryEntry>::new();
    let d3 = zfs_download_digest_dir();
    let dpath = std::path::Path::new(&d3);
    loop {
        async_std::task::sleep(SANITIZER_PERIOD).await;
        log::debug!("Running Sanitizer...");
        if let Ok(entries) = dpath.read_dir() {
            for entry in entries.flatten() {
                log::debug!("Sanitizer looking into <{:?}>", &entry);
                match registry.get_mut(entry.path().to_str().unwrap()) {
                    Some(reg_entry) => {
                        log::debug!("Registry {:?} exists for  <{:?}>", &reg_entry, &entry);
                        if let Ok(gap_set) = compute_download_gaps(&reg_entry.digest).await {
                            let mut gaps: Vec<usize> = gap_set.into_iter().collect();
                            if gaps.is_empty() {
                                log::debug!("Found <<NO GAPS>> for {:?}", &reg_entry.digest);
                                cleanup_download(&reg_entry.digest, entry.path().to_str().unwrap())
                                    .await
                                    .unwrap();
                            } else {
                                log::debug!("Found <<GAPS>> for {:?}", &reg_entry.digest);
                                gaps.sort_unstable();
                                let new_gap_num = gaps.len();
                                let filtered_gaps: Vec<usize> = gaps
                                    .clone()
                                    .into_iter()
                                    .filter(|n| *n >= reg_entry.tide_level)
                                    .collect();

                                let delta = reg_entry.gap_nun - new_gap_num;
                                log::debug!("Gaps delta is :\n\t{:?}", delta);
                                if delta > 0 {
                                    log::debug!("Udating tide and gaps");
                                    (*reg_entry).tide_level = *filtered_gaps.get(0).unwrap();
                                    (*reg_entry).gap_nun = new_gap_num;
                                    // registry.insert(
                                    //     entry.path().to_str().unwrap().into(),
                                    //     reg_entry.clone(),
                                    // );
                                } else {
                                    (*reg_entry).stuck_cycles += 1;
                                    if reg_entry.stuck_cycles % STUCK_CYCLES_RESET == 0 {
                                        log::debug!(
                                            "Gaps recovery for {:?} seems to have stalled, this may be due to process restart of disconnections. Restarting fragment sanitiser.",
                                            &reg_entry.digest.key);
                                        (*reg_entry).tide_level = 0;
                                        let n = std::cmp::min(
                                            gaps.len(),
                                            GAP_DOWNLOAD_SCHEDULE
                                                * compute_acceleration_factor(
                                                    reg_entry.stuck_cycles,
                                                ),
                                        );
                                        for i in 0..n {
                                            reg_entry.tide_level = *gaps.get(i).unwrap();
                                            async_std::task::spawn(download_fragment(
                                                z.clone(),
                                                reg_entry.digest.key.clone(),
                                                reg_entry.tide_level as u32,
                                            ));
                                        }
                                    } else {
                                        (*reg_entry).stuck_cycles += 1;
                                        log::debug!(
                                            "Gaps recovery for {:?} is unusually slow -- no progress for the past {} sanitiser cycles",
                                            &reg_entry.digest.key, reg_entry.stuck_cycles
                                        );
                                    }
                                }
                            }
                        } else {
                            log::debug!("Unable to compute gap for {:?}, the fragmentation manifest may be missing...", entry);
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
                        gaps.sort_unstable();

                        if !gaps.is_empty() {
                            let tide_level = *gaps.get(0).unwrap();
                            let gap_nun = gaps.len();
                            let sre = SanitizerRegistryEntry {
                                digest: Arc::new(digest),
                                tide_level,
                                gap_nun,
                                stuck_cycles: 0,
                            };
                            log::debug!(
                                "Created registry entry {:?} exists for  <{:?}>",
                                &sre,
                                &entry.path()
                            );
                            registry.insert(entry.path().to_str().unwrap().into(), sre);
                        } else {
                            log::debug!("Sanitizer cleaning up download {:?}", &digest);
                            cleanup_download(&digest, entry.path().to_str().unwrap())
                                .await
                                .unwrap();
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
