use async_std::fs::{create_dir_all, File};
use async_std::prelude::*;
use checksum::crc::Crc;
use std::path::Path;
use std::path::PathBuf;
use crate::*;

pub async fn fragment(
    file_path: &str,
    zkey: &str,
    fragment_size: usize,
) -> Result<crate::FragmentationDigest, String> {
    match Crc::new(file_path).checksum() {
        Ok(checksum) => {
            let mut file = match File::open(file_path).await {
                Ok(f) => f,
                Err(_) => return Err(format!("Unable to open the file {}", file_path)),
            };
            let mut bs = vec![0_u8; fragment_size];
            log::debug!("bs.len() = {}", bs.len());
            let mut fid = 0;
            let frag_path = zfs_upload_frags_dir_for_key(zkey);
            log::debug!("Target dir: {:?}", frag_path);
            create_dir_all(Path::new(&frag_path)).await.unwrap();
            loop {
                match file.read(&mut bs).await {
                    Ok(n) if n > 0 => {
                        let fname = format!("{}/{}", &frag_path, fid.to_string());
                        let mut f = match File::create(&fname).await {
                            Ok(f) => f,
                            Err(e) => {
                                log::debug!("Error {:?} while creating the fragment: {}", e, &fname);
                                panic!("IO Error")
                            }
                        };
                        let _ignore = f.write(&bs[0..n]).await;
                        fid += 1;
                    }
                    _ => break,
                }
            }
            let digest = crate::FragmentationDigest {
                name: zkey.into(),
                crc: checksum.crc64,
                fragment_size,
                fragments: fid,
            };
            log::debug!("{:?}", digest);
            write_defrag_digest(&digest, &frag_path).await.map(|_| digest)
        }
        Err(e) => Err(e.into()),
    }
}

pub async fn fragment_from_digest(path: String, fragment_size: usize) -> Result<(), String> {
    let path = PathBuf::from(path);
    let mut target = PathBuf::from(path.parent().unwrap());
    target.push(crate::FRAGS_SUBDIR);

    let bs = std::fs::read(path.as_path()).unwrap();
    let upload_spec = match serde_json::from_slice::<crate::UploadDigest>(&bs) {
        Ok(us) => us,
        Err(e) => return Err(format!("{:?}", e)),
    };
    log::debug!(target: "zfsd", "Uploading: {} as {}", &upload_spec.path, &upload_spec.key);
    if !std::path::Path::new(&upload_spec.path).exists() {
        log::warn!(target: "zfsd", "The file {} does not exit", &upload_spec.path);
        return Ok(())
    }
    crate::frag::fragment(
        &upload_spec.path,
        &upload_spec.key,
        fragment_size,
    )
    .await
    .unwrap();
    Ok(())
}

pub async fn read_defrag_digest(base_path: &str) -> Result<FragmentationDigest, String> {
    let path: PathBuf = [&base_path, crate::ZFS_DIGEST].iter().collect();
    let bs = async_std::fs::read(path.as_path()).await.unwrap();
    match serde_json::from_slice::<crate::FragmentationDigest>(&bs) {
        Ok(digest) => Ok(digest),
        Err(e) => Err(format!("{:?}", e))
    }
}

pub async fn write_defrag_digest(digest: &FragmentationDigest, base_path: &str) -> Result<(), String> {
    let bs = serde_json::to_vec(&digest).unwrap();
    let digest_path = format!("{}/{}", base_path, ZFS_DIGEST);
    let mut fdigest = File::create(Path::new(&digest_path)).await.unwrap();
    fdigest.write(&bs).await.unwrap();
    Ok(())
}
pub async fn defragment(key: &str, dest: &str) -> Result<bool, String> {
    let fragments_path = zfs_download_frags_dir_for_key(key);

    match read_defrag_digest(&fragments_path).await {
        Ok(digest) => {
            let dest_path = Path::new(dest);
            let dest_dir =
                dest_path.parent().unwrap().to_str().unwrap().to_string();
            create_dir_all(Path::new(&dest_dir)).await.unwrap();

            let mut f = File::create(Path::new(&dest)).await.unwrap();
            for i in 0..digest.fragments {
                let frag_path = format!("{}/{}", fragments_path, i);
                let bs = std::fs::read(Path::new(&frag_path)).unwrap();
                f.write(&bs).await.unwrap();
            }

            drop(f);
            let crc64 = Crc::new(dest)
                .checksum()
                .unwrap()
                .crc64;
            Ok(crc64 == digest.crc)
        },
        Err(e) => Err(e)
    }
}
