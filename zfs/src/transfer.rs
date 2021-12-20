use futures::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::*;
use indicatif::{ProgressBar, ProgressStyle};
use zenoh::net::{queryable, QueryConsolidation, QueryTarget, Session, Target};

pub async fn upload_fragment(z: &Session, path: &str, key: &str) {
    let path = PathBuf::from(path);
    let bs = std::fs::read(path.as_path()).unwrap();
    z.write(&key.into(), bs.into()).await.unwrap();
}

pub async fn download_fragment(z: Arc<Session>, key: String, n: u32) -> Result<(), String> {
    let path = zfs_download_frags_dir_for_key(&key);
    let frag_key = format!("{}/{}", key, n);
    let frag = format!("{}/{}", &path, n);

    if Path::new(&frag).exists() {
        log::debug!(
            "The fragment {} already has already been downloaded, skipping.",
            &frag
        );
        return Ok(());
    }

    // First check if the fragment is already there -- there is potential concurrency between
    // the sanitizer and the regular download process.
    log::debug!(target: "zfsd", "Retrieving fragment: {}/{}", key, n);
    let mut replies = z
        .query(
            &frag_key.clone().into(),
            "",
            QueryTarget {
                kind: queryable::STORAGE,
                target: Target::default(),
            },
            QueryConsolidation::default(),
        )
        .await
        .unwrap();
    if let Some(reply) = replies.next().await {
        let bs = reply.data.payload.contiguous();
        async_std::fs::write(std::path::Path::new(&frag), bs)
            .await
            .map_err(|e| format!("{:?}", e))
    } else {
        Err(format!("Unable to retrieve fragment: {}", &frag_key))
    }
}
pub async fn download_fragmentation_digest(
    z: std::sync::Arc<Session>,
    digest_key: &str,
) -> Result<FragmentationDigest, String> {
    log::debug!(target: "zfsd", "Retrieving fragmentation digest: {}", &digest_key);
    let mut replies = z
        .query(
            &digest_key.into(),
            "",
            QueryTarget {
                kind: queryable::STORAGE,
                target: Target::default(),
            },
            QueryConsolidation::default(),
        )
        .await
        .unwrap();

    if let Some(reply) = replies.next().await {
        let bs = reply.data.payload.contiguous();
        let digest = serde_json::from_slice::<FragmentationDigest>(&bs).unwrap();
        Ok(digest)
    } else {
        Err("Unable to retriefe manifest".to_string())
    }
}

pub async fn download(
    z: std::sync::Arc<Session>,
    path_buf: PathBuf,
    pstyle: ProgressStyle,
) -> Result<(), String> {
    // let path = path_buf.as_path();
    let bs = std::fs::read(path_buf.as_path()).unwrap();
    let download_spec = match serde_json::from_slice::<DownloadDigest>(&bs) {
        Ok(ds) => ds,
        Err(e) => return Err(format!("{:?}", e)),
    };

    if std::path::Path::new(&download_spec.path).exists() {
        println!(
            "The file {} has already been downloaded.",
            &download_spec.path
        );
        return Ok(());
    }

    let frag_digest = format!("{}/{}", download_spec.key, ZFS_DIGEST);
    let digest = download_fragmentation_digest(z.clone(), &frag_digest).await?;

    let frags_dir = zfs_download_frags_dir_for_key(&download_spec.key);
    async_std::fs::create_dir_all(std::path::Path::new(&frags_dir))
        .await
        .unwrap();

    let bar = ProgressBar::new(digest.fragments.into());
    bar.set_style(pstyle);
    bar.set_message(download_spec.key.clone());
    write_defrag_digest(&digest, &frags_dir).await?;
    for i in 0..digest.fragments {
        download_fragment(z.clone(), download_spec.key.clone(), i).await?;
        bar.inc(1);
    }

    log::debug!(target: "zfsd", "Degragmenting into {}", &download_spec.path);
    let p = std::path::Path::new(&download_spec.path);
    match p.parent() {
        Some(parent) => {
            std::fs::create_dir_all(parent).unwrap();
            defragment(&download_spec.key, &download_spec.path)
                .await
                .and_then(|r| {
                    if r {
                        Ok(bar.finish())
                    } else {
                        Ok(log::warn!(
                            "The file received for {} was currupted.",
                            &download_spec.key
                        ))
                    }
                })
        }
        None => {
            log::warn!(target: "zfsd", "Invalid target path: {:?}\n Unable to defragment", p);
            bar.finish_with_message("failed to defragment (see log)");
            Ok(())
        }
    }
}
