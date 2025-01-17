use clap::{App, Arg};
use futures::TryFutureExt;
use notify::{recommended_watcher, RecursiveMode, Result, Watcher};
use std::fs::create_dir_all;
use std::{sync::mpsc::channel};
use std::process::exit;
use zfs::*;
use zenoh::config::WhatAmI;

fn init() -> Result<()> {
    create_dir_all(zfsd_upload_frags_dir())
        .and(create_dir_all(zfsd_download_frags_dir()))
        .and(create_dir_all(zfsd_upload_digest_dir()))
        .and(create_dir_all(zfsd_download_digest_dir()))
        .map_err(|e| notify::Error::generic(&format!("{:?}", e)))
}

#[tokio::main]
async fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .format_target(true)
        .format_timestamp_secs()
        .init();

    log::info!(target: "zfsd", "Starting up...");
    let zconf = parse_args();

    let z = std::sync::Arc::new(zenoh::open(zconf).await.unwrap());
    init().expect("zfsd failed to initalise!");
    let (tx, rx) = channel();
    let mut watcher = recommended_watcher(tx).unwrap();

    watcher
        .watch(
            std::path::Path::new(&zfsd_download_digest_dir()),
            RecursiveMode::NonRecursive)
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_digest_dir()),
            RecursiveMode::NonRecursive)
        .unwrap();
    watcher
        .watch(
            std::path::Path::new(&zfsd_upload_frags_dir()),
            RecursiveMode::Recursive)
        .unwrap();

    tokio::task::spawn(download_sanitizer(z.clone()));

    log::info!(target:"zfsd", "Up and Running!");
    while let Ok(r) = rx.recv() {
        if let Ok(evt) = r {
            if evt.kind.is_create() && evt.paths[0].is_file() {
                log::debug!(target: "zfsd", "Received Create Event {:?}", &evt);
                let path = evt.paths[0].clone();
                let parent = path.parent().unwrap();

                if parent.ends_with(DOWNLOAD_SUBDIR) {
                    log::info!(target: "zfsd", "Downloading {:?}", &path);
                    tokio::task::spawn(
                        zfs::download(z.clone(), path.clone()).or_else(
                            |e| async move {
                                log::warn!("Failed to download due to: {}", e);
                                Ok::<(), String>(())
                            },
                        ),
                    );
                } else if parent.ends_with(UPLOAD_SUBDIR) {
                    log::info!(target: "zfsd","Fragmenting {:?}", &path);
                    let p = path.to_str().unwrap().to_string();
                    let _ignore = tokio::task::spawn(zfs::fragment_from_digest(p).or_else(
                        |e| async move {
                            log::warn!("Failed to fragment due to: {}", e);
                            Ok::<(), String>(())
                        },
                    ));
                } else {
                    let fpath = path.to_str().unwrap();
                    if !fpath.contains(DOWNLOAD_SUBDIR) {
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => {
                                match zfsd_upload_frag_dir_to_key(fpath) {
                                    Some(key_suffix) => {
                                        let key = zfs_key(&key_suffix);
                                        log::debug  !(target: "zfsd", "Uploading fragment : {:?} as {:?}", path, &key);
                                        upload_fragment(&z, fpath, &key).await;
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
            } else {
                log::debug!(target: "zfsd", "Ignoring create event for directory {:?}", &evt);
            }
        }
    }
}

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
        .map_or_else(| | { zenoh::Config::default() }, |conf_file| {
            zenoh::Config::from_file(conf_file).unwrap()
        });

    if let Some(mode) = args.value_of("mode") {
        if mode == "peer" {
            config.set_mode(Some(WhatAmI::Peer)).unwrap();
        } else if mode == "client" {
            config.set_mode(Some(WhatAmI::Client)).unwrap();
        } else {
            println!("Invalid mode: {}", mode);
            exit(-1);
        }
    }
    if let Some(values) = args.values_of("remote-endpoints") {
        config.connect.endpoints.set(
            values.map(|v| v.parse().unwrap()).collect()
        ).expect("Invalid Endpoints");
    }

    config
}