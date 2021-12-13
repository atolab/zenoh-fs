use std::fs::{create_dir_all};
use std::collections::{HashMap, BTreeSet};
use clap::{App, Arg};
use indicatif::ProgressStyle;
use notify::{DebouncedEvent, RecursiveMode, Watcher};
use std::{sync::mpsc::channel, sync::Arc, time::Duration};
use zenoh::net::*;
use zenoh::Properties;
use zfs::*;

fn parse_args() -> Properties {
    let args = App::new("zenoh distributed file sytem")
        .arg(Arg::from_usage(
            "-s, --fragment-size=[size]...  'The maximun size used for fragmenting for files.'",
        ))
        .arg(Arg::from_usage(
            "-r, --remote-endpoints=[ENDPOINTS]...  'The locators for a remote zenoh endpoint such as a routers'",
        ))
        .get_matches();

    let mut config = Properties::default();
    if let Some(values) = args.values_of("remote-endpoints") {
        config.insert(
            "remote-endpoints".to_string(),
            values.collect::<Vec<&str>>().join(","),
        );
    }

    config
}

fn init() -> Result<(), String> {
    create_dir_all(zfs_upload_frags_dir())
        .and(create_dir_all(zfs_download_frags_dir()))
        .and(create_dir_all(zfs_upload_digest_dir()))
        .and(create_dir_all(zfs_download_digest_dir()))
        .map_err(|e| format!("{:?}", e))
}

async fn cleanup_download_if_complete(_manifest: &DownloadDigest) {

}
async fn cleanup_download(_digest: &DownloadDigest) -> Result<(), String> {
    Ok(())
}
async fn is_download_completed(digest: &DownloadDigest) -> bool { true }

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
                let name = entry.path().file_name()
                    .and_then(|s| s.to_str()).unwrap().to_string();
                if let Ok(n) = name.parse() {
                    frag_set.remove(&n);
                }
            }
        }
    }
    Ok(frag_set)
}
struct SanitizerRegistryEntry {
    digest: Arc<DownloadDigest>,
    tide_level: usize,
    progress: bool
}

async fn download_sanitizer(z: Arc<Session>) {
    let mut registry = HashMap::<String, SanitizerRegistryEntry>::new();
    let d3 = zfs_download_digest_dir();
    let dpath = std::path::Path::new (&d3);
    loop {
        async_std::task::sleep(SANITIZER_PERIOD).await;
        if let Ok(entries) = dpath.read_dir() {
            for e in entries {
                if let Ok(entry) = e {
                    match registry.get_mut(entry.path().to_str().unwrap()) {
                        Some (reg_entry) => {
                            // &mut (digest, mut tide_level, mut progress)
                            let mut gaps: Vec<usize> =
                                compute_download_gaps(&reg_entry.digest).await.unwrap()
                                    .into_iter()
                                    .collect();

                            gaps.sort();

                            let filtered_gaps : Vec<usize> =
                                gaps.clone().into_iter()
                                    .filter(|n| *n >= reg_entry.tide_level)
                                    .collect();

                            if filtered_gaps.len() != 0 {

                                if !gaps.contains(&reg_entry.tide_level) {
                                    let mut n = GAP_DOWNLOAD_SCHEDULE;
                                    for gap in gaps {
                                        if n > 0 {
                                            reg_entry.tide_level = gap;
                                            async_std::task::spawn(download_fragment(z.clone(), reg_entry.digest.key.clone(), gap as u32));
                                        }
                                    }
                                } else {
                                    log::warn!("Gaps recovery for {:?} is unusually slow", &reg_entry.digest.key);
                                }
                            } else if gaps.len() == 0 {
                                log::info!("Gaps recovery for {:?} is unusually slow", &reg_entry.digest.key);
                            }
                        },
                        None => {
                            let digest =
                                zfs_read_download_digest_from(entry.path().as_path()).await.unwrap();
                            let mut gaps: Vec<usize> =
                                compute_download_gaps(&digest).await.unwrap()
                                    .into_iter()
                                    .collect();
                            gaps.sort();


                            if gaps.len() > 0 {
                                let tide_level = *gaps.get(0).unwrap();
                                let progress = true;
                                let sre = SanitizerRegistryEntry {
                                    digest: Arc::new(digest),
                                    tide_level,
                                    progress
                                };
                                registry.insert(entry.path().to_str().unwrap().into(), sre);
                            } else {
                                cleanup_download(&digest).await.unwrap();
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
async fn upload_sanitizer() {
    loop {
        async_std::task::sleep(Duration::from_secs(5)).await;
        // async_std::fs::read_dir()
    }
}

#[async_std::main]
async fn main() {
    println!("Starting zfsd...");
    env_logger::init();
    let zconf = parse_args();

    let z = std::sync::Arc::new(open(zconf.into()).await.unwrap());
    init().expect("zfsd failed to initalise!");
    let (tx, rx) = channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(FS_EVT_DELAY)).unwrap();
    let fragment_size = FRAGMENT_SIZE;
    watcher.watch(&zfs_download_digest_dir(), RecursiveMode::NonRecursive).unwrap();
    watcher.watch(&zfs_upload_digest_dir(), RecursiveMode::NonRecursive).unwrap();
    watcher.watch(&zfs_upload_frags_dir(), RecursiveMode::Recursive).unwrap();

    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    println!("zfsd is up an running.");
    while let Ok(evt) = rx.recv() {
        if let DebouncedEvent::Create(path) = evt {
            if path.is_file() {
                let parent = path.parent().unwrap();

                if parent.ends_with(DOWNLOAD_SUBDIR) {
                    log::info!(target: "zfsd", "Downloading {:?}", &path);
                    // Note: The only reason for not spawning an async task is that the
                    // indicatif crate does not work properly show progress when using multibar
                    // along with async tasks.

                    match zfs::download(z.clone(), path.as_path(), sty.clone()).await {
                        Ok(()) => {}
                        Err(e) => {
                            log::warn!("Failed to download due to: {:?}", e);
                        }
                    }
                    // std::fs::remove_file(path.as_path()).unwrap();
                } else if parent.ends_with(UPLOAD_SUBDIR) {
                    log::info!(target: "zfsd","Fragmenting {:?}", &path);
                    let p = path.to_str().unwrap().to_string();
                    let _ignore =
                        async_std::task::spawn(zfs::fragment_from_digest(p, fragment_size));
                    println!("Fragmenting and uploading {:?}", path.as_path());
                } else {
                    let fpath = path.to_str().unwrap();
                    if !fpath.contains(DOWNLOAD_SUBDIR) {
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => {
                                log::debug!(target: "zfsd", "Handling path: {}", fpath);
                                let fname: String = if let Some(s) = path.file_name() {
                                    s.to_str().unwrap().to_string()
                                } else { break };
                                match zfs_upload_frag_dir_to_key(fpath) {
                                    Some(key) => {
                                        log::debug!(target: "zfsd", "Uploading fragment : {:?} as {:?}", path, &key);
                                        upload_fragment(&*z, fpath, &key).await;
                                        log::debug!(target: "zfsd", "Completed  upload of: {:?} as {:?}", path, &key);
                                    }
                                    None => {
                                        log::warn!(target: "zfsd", "Unable to extract key from {}", fpath);
                                    }
                                }
                            }
                            None => {
                                log::warn!(target: "zfsd", "Ignoring {:?} path...", &path);
                            }
                        }
                    }
                }
            }
        }
    }
}
