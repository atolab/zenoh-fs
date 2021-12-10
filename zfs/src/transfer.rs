use async_std::fs::OpenOptions;
use futures::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};
use zenoh::net::{queryable, QueryConsolidation, QueryTarget, Session, Target};

pub async fn upload_fragment(z: &Session, path: &str, key: &str) {
    let path = PathBuf::from(path);
    let bs = std::fs::read(path.as_path()).unwrap();
    z.write(&key.into(), bs.into()).await.unwrap();
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
        println!("The file {} has already been downloaded.", &download_spec.path);
        return Ok(())
    }

    let manifest = format!("{}/{}", download_spec.key, crate::ZFS_DIGEST);
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
        let digest = serde_json::from_slice::<crate::FragmentationDigest>(&bs).unwrap();
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
