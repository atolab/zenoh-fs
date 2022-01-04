use clap::{App, Arg};
use futures::TryFutureExt;
use indicatif::ProgressStyle;
use notify::{DebouncedEvent, RecursiveMode, Watcher};
use zenoh::prelude::{Receiver, ZFuture};
use std::fs::create_dir_all;
use std::{sync::mpsc::channel, time::Duration};
use zenoh::config::Config;
use zfs::*;

fn parse_args() -> zenoh::config::Config {
    let args = App::new("zenoh distributed file sytem")
        .arg(Arg::from_usage(
            "-m, --mode=[MODE] 'The zenoh session mode (peer by default)."
        ).possible_values(&["peer", "client"]))
        .arg(Arg::from_usage(
            "-c, --config=[FILE]  'A zenoh configuration file.'",
        ))
        .arg(Arg::from_usage(
            "-s, --fragment-size=[size]  'The maximun size used for fragmenting for files.'",
        ))
        .arg(Arg::from_usage(
            "-r, --remote-endpoints=[ENDPOINTS]...  'The locators for a remote zenoh endpoint such as a routers'",
        ))
        .get_matches();

    let mut config = args
        .value_of("config")
        .map_or_else(Config::default, |conf_file| {
            Config::from_file(conf_file).unwrap()
        });
    if let Some(Ok(mode)) = args.value_of("mode").map(|mode| mode.parse()) {
        config.set_mode(Some(mode)).unwrap();
    }
    if let Some(values) = args.values_of("remote-endpoints") {
        config.peers.extend(values.map(|v| v.parse().unwrap()));
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

#[async_std::main]
async fn main() {
    println!("Starting zfsd...");
    env_logger::init();
    let zconf = parse_args();

    let z = std::sync::Arc::new(zenoh::open(zconf).await.unwrap());
    let pid = z.info().await.get(&zenoh::info::ZN_INFO_PID_KEY).expect("zfsd failed to retrieve it's zenoh pid!").clone();
    init().expect("zfsd failed to initalise!");
    let (tx, rx) = channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(FS_EVT_DELAY)).unwrap();
    watcher
        .watch(&zfs_download_digest_dir(), RecursiveMode::NonRecursive)
        .unwrap();
    watcher
        .watch(&zfs_upload_digest_dir(), RecursiveMode::NonRecursive)
        .unwrap();
    watcher
        .watch(&zfs_upload_frags_dir(), RecursiveMode::Recursive)
        .unwrap();

    let sty = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
        .progress_chars("##-");

    async_std::task::spawn(download_sanitizer(z.clone()));

    async_std::task::spawn_blocking({
        let z = z.clone();
        move || {
            let mut sub = z.subscribe(&format!("/@/zfs/{}/config/**", pid)).wait().expect(&format!("zfsd failed to subscribe to /@/zfs/{}/config/**", pid));
            while let Ok(sample) = sub.receiver().recv() {
                let conf_key = sample.key_expr.as_str().split('/').last().unwrap();
                let conf_val = String::from_utf8_lossy(&sample.value.payload.contiguous()).to_string();
                let _ = z.config().wait().insert_json(&conf_key, &conf_val);
            }
        }
    });

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

                    async_std::task::spawn(
                        zfs::download(z.clone(), path.clone(), sty.clone()).or_else(
                            |e| async move {
                                log::warn!("Failed to download due to: {}", e);
                                Ok::<(), String>(())
                            },
                        ),
                    );
                } else if parent.ends_with(UPLOAD_SUBDIR) {
                    log::info!(target: "zfsd","Fragmenting {:?}", &path);
                    let p = path.to_str().unwrap().to_string();
                    let _ignore = async_std::task::spawn(zfs::fragment_from_digest(p).or_else(
                        |e| async move {
                            log::warn!("Failed to fragment due to: {}", e);
                            Ok::<(), String>(())
                        },
                    ));
                    println!("Fragmenting and uploading {:?}", path.as_path());
                } else {
                    let fpath = path.to_str().unwrap();
                    if !fpath.contains(DOWNLOAD_SUBDIR) {
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => {
                                log::debug!(target: "zfsd", "Handling path: {}", fpath);
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
