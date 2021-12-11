use async_std::fs::OpenOptions;
use futures::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};
use zenoh::net::{queryable, QueryConsolidation, QueryTarget, Session, Target};
use crate::{defragment, DownloadDigest, FragmentationDigest, zfs_download_frags_dir};

pub async fn upload_fragment(z: &Session, path: &str, key: &str) {
    let path = PathBuf::from(path);
    let bs = std::fs::read(path.as_path()).unwrap();
    z.write(&key.into(), bs.into()).await.unwrap();
}

pub async fn download_fragment(z: Arc<Session>, key: &str, path: &str, n: u32) -> Result<(), String> {
    log::debug!(target: "zfsd", "Retrieving fragment: {}/{}", key, n);
    let frag_key = format!("{}/{}", key, n);
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
        let frag = format!("{}/{}", &path, n);
        async_std::fs::write(std::path::Path::new(&frag), bs).await
            .map_err(|e| format!("{:?}", e))
    } else {
        Err(format!("Unable to retrieve fragment: {}", &frag_key))
    }
}
pub async fn download_fragmentation_digest(z: std::sync::Arc<Session>, digest_key: &str) -> Result<FragmentationDigest, String> {
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
        let digest = serde_json::from_slice::<crate::FragmentationDigest>(&bs).unwrap();
        Ok(digest)
    } else {
        Err("Unable to retriefe manifest".to_string())
    }
}

pub async fn download(
    z: std::sync::Arc<Session>,
    path: Arc<PathBuf>,
    pstyle: ProgressStyle,
) -> Result<(), String> {
    let bs = std::fs::read(path.as_path()).unwrap();
    let download_spec = match serde_json::from_slice::<crate::DownloadDigest>(&bs) {
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

    let frag_digest =  format!("{}/{}", download_spec.key, crate::ZFS_DIGEST);
    let digest = download_fragmentation_digest(z.clone(), &frag_digest).await?;

    let frags_dir = crate::zfs_download_frags_dir_for_key(&download_spec.key);
    async_std::fs::create_dir_all(std::path::Path::new(&frags_dir)).await.unwrap();


    let bar = ProgressBar::new(digest.fragments.into());
    bar.set_style(pstyle);
    bar.set_message(download_spec.key.clone());
    for i in 0..digest.fragments {
        download_fragment(z.clone(), &download_spec.key, &frags_dir, i).await?;
        bar.inc(1);
    }
    crate::write_defrag_digest(&digest,&frags_dir).await?;
    log::debug!(target: "zfsd", "Degragmenting into {}", &download_spec.path);
    let p = std::path::Path::new(&download_spec.path);
    match p.parent() {
        Some(parent) => {
            std::fs::create_dir_all(parent);
            defragment(&download_spec.key, &download_spec.path).await;
            bar.finish();
        },
        None => {
            log::warn!(target: "zfsd", "Invalid target path: {:?}\n Unable to defragment", p);
            bar.finish_with_message("failed to defragment (see log)");
        }
    }

    Ok(())
}
