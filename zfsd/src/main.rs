use async_std::fs::{create_dir_all, OpenOptions};
use clap::{App, Arg};
use futures::prelude::*;
use notify::{DebouncedEvent, RecursiveMode, Watcher};
use std::path::PathBuf;
use std::{sync::mpsc::channel, sync::Arc, time::Duration};
use zenoh::net::*;
use zenoh::Properties;
use indicatif::{ProgressBar, ProgressStyle};


const EVT_DELAY: u64 = 1;
const DOWNLOAD_SUBDIR: &str = "download";
const UPLOAD_SUBDIR: &str = "upload";
const FRAGS_SUBDIR: &str = "frags";
const FRAGMENT_SIZE: usize = 4 * 1024;

fn parse_args() -> (String, Properties) {
    let args = App::new("zenoh distributed file sytem")
        .arg(
            Arg::from_usage("-p, --path=[PATH] 'The working directory for zfd")  
            .default_value("/~/.zfs/")              
        )
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

    let path = args.value_of("path").unwrap().to_string();

    (path, config)
}

async fn init(path: &str) -> Result<(), String> {
    let mut upld_dir = PathBuf::from(path);
    upld_dir.push(UPLOAD_SUBDIR);
    let mut dwnld_dir = PathBuf::from(path);
    dwnld_dir.push(DOWNLOAD_SUBDIR);
    create_dir_all(upld_dir.as_path()).await.unwrap();
    create_dir_all(dwnld_dir.as_path()).await.unwrap();

    Ok(())
}

async fn fragment(path: String, fragment_size: usize) -> Result<(), String> {
    let path = PathBuf::from(path);
    let mut target = PathBuf::from(path.parent().unwrap());
    target.push(FRAGS_SUBDIR);

    let bs = std::fs::read(path.as_path()).unwrap();
    let upload_spec = match serde_json::from_slice::<zfs::UploadDigest>(&bs) {
        Ok(us) => us,
        Err(e) => return Err(format!("{:?}", e)),
    };
    log::debug!(target: "zfsd", "Uploading: {} as {}", &upload_spec.path, &upload_spec.key);
    zfs::frag::fragment(
        &upload_spec.path,
        target.to_str().unwrap(),
        &upload_spec.key,
        fragment_size,
    )
    .await
    .unwrap();
    Ok(())
}
async fn upload_fragment(z: &Session, path: &str, key: &str) {
    let path = PathBuf::from(path);
    let bs = std::fs::read(path.as_path()).unwrap();
    z.write(&key.into(), bs.into()).await.unwrap();
}

async fn download(z: std::sync::Arc<Session>, path: Arc<PathBuf>, pstyle: ProgressStyle) -> Result<(), String> {
    let bs = std::fs::read(path.as_path()).unwrap();
    let download_spec = match serde_json::from_slice::<zfs::DownloadDigest>(&bs) {
        Ok(ds) => ds,
        Err(e) => return Err(format!("{:?}", e)),
    };

    let manifest = format!("{}/{}", download_spec.key, zfs::ZFS_DIGEST);
    log::debug!(target: "zfsd", "Retrieving manifest: {}", &manifest);
    let mut replies = z
        .query(
            &manifest.into(),
            "",
            QueryTarget {
                kind: queryable::STORAGE,
                target: Target::default(),
            },
            QueryConsolidation::default(),
        )
        .await
        .unwrap();
    let temp = format!("{}-zfs-temp", download_spec.path);
    let mut ftemp = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&temp)
        .await
        .unwrap();
    if let Some(reply) = replies.next().await {
        let bs = reply.data.payload.contiguous();
        let digest = serde_json::from_slice::<zfs::FragmentationDigest>(&bs).unwrap();                
        let bar = ProgressBar::new(digest.fragments.into());   
        bar.set_style(pstyle);     
        bar.set_message(download_spec.key.clone());                
        for i in 0..digest.fragments {
            let path = format!("{}/{}", download_spec.key, i);
            log::debug!(target: "zfsd", "Retrieving fragment: {}", &path);
            let mut replies = z
                .query(
                    &path.into(),
                    "",
                    QueryTarget {
                        kind: queryable::STORAGE,
                        target: Target::default(),
                    },
                    QueryConsolidation::default(),
                )
                .await
                .unwrap();
            bar.inc(1);
            if let Some(reply) = replies.next().await {
                let bs = reply.data.payload.contiguous();
                ftemp.write(&bs).await.unwrap();
            }
            ftemp.close().await.unwrap();
        }
        bar.finish();
        log::debug!(target: "zfsd", "Renaming {} into {}", &temp, &download_spec.path);
        async_std::fs::rename(&temp, &download_spec.path)
            .await
            .unwrap();
    }
    Ok(())
}

#[async_std::main]
async fn main() {
    println!("Starting zfsd...");
    env_logger::init();
    let (path, zconf) = parse_args();

    let z = std::sync::Arc::new(open(zconf.into()).await.unwrap());
    init(&path).await.unwrap();
    let (tx, rx) = channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(EVT_DELAY)).unwrap();
    let fragment_size = FRAGMENT_SIZE;
    watcher.watch(&path, RecursiveMode::Recursive).unwrap();
    let mut frags_dir = PathBuf::from(path);
    frags_dir.push(UPLOAD_SUBDIR);
    frags_dir.push(FRAGS_SUBDIR);
    
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
                    match download(z.clone(), Arc::new(path.clone()), sty.clone()).await {
                        Ok(()) => {}
                        Err(e) => {
                            log::warn!("Failed to download due to: {:?}", e);
                        }
                    }
                } else if parent.ends_with(UPLOAD_SUBDIR) {
                    log::info!(target: "zfsd","Fragmenting {:?}", &path);
                    let p = path.to_str().unwrap().to_string();
                    let _ignore = async_std::task::spawn(fragment(p, fragment_size));
                } else {
                    let fpath = path.to_str().unwrap();
                    match fpath.find(FRAGS_SUBDIR) {
                        Some(_) => {
                            let key = fpath.strip_prefix(frags_dir.to_str().unwrap()).unwrap();
                            log::debug!(target: "zfsd", "Uploading fragment : {:?} as {:?}", path, key);
                            upload_fragment(&*z, fpath, key).await;
                            log::debug!(target: "zfsd", "Completed  upload of: {:?} as {:?}", path, key);
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
