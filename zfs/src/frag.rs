use async_std::fs::{File, create_dir_all};
use async_std::prelude::*;
// use std::io::Read;
// use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use checksum::{crc::Crc};
use serde_json;

pub async fn fragment(file_path: &str, dest_path: &str, zkey: &str, fragment_size: usize) -> Result<crate::FragmentationDigest, String> {
    
    match Crc::new(file_path).checksum() {
        Ok(checksum) => {
            let mut file = 
                match File::open(file_path).await {
                    Ok(f) => f,
                    Err(_) => return Err(format!("Unable to open the file {}", file_path).into())
                };
            let mut bs = Vec::<u8>::with_capacity(fragment_size);
            for _ in 0..fragment_size {
                bs.push(0);
            }            
            println!("bs.len() = {}", bs.len());            
            let mut fid = 0;
            
            let k = Path::new(zkey);            
            let mut target: PathBuf = [
                dest_path, 
                k.strip_prefix(Path::new("/")).unwrap().to_str().unwrap()].iter().collect();
            println!("Target dir: {:?}", target);
            create_dir_all(target.as_path()).await.unwrap();
            loop {
                match file.read(&mut bs).await {
                    Ok(n) if n > 0 => {
                        let mut frag_target = target.clone();
                        frag_target.push(fid.to_string());
                        let fname = frag_target.to_str().unwrap();
                        let mut f =  match File::create(fname).await {
                            Ok(f) => f,
                            Err(e) => {
                                println!("Error {:?} while creating the fragment: {}", e, fname);
                                panic!("IO Error")
                            }
        
                        };
                        let _ = f.write(&bs[0..n]).await;
                        fid += 1;
                    },
                    _ => break
                }    
            }
            
            let digest = crate::FragmentationDigest { 
                name: zkey.into(), 
                crc: checksum.crc64, 
                fragment_size, 
                fragments: fid};
            let bs = serde_json::to_vec(&digest).unwrap();
            target.push(crate::ZFS_DIGEST);
            let mut fdigest = File::create(target.as_path()).await.unwrap();
            fdigest.write(&bs).await.unwrap();
            println!("{:?}", digest);
            Ok(digest)
            
        },
        Err(e) => Err(e.into())
    }
    
}
pub async fn defragment(fragments_path: &str, dest_path: &str) -> bool {
    let path: PathBuf = [fragments_path, crate::ZFS_DIGEST].iter().collect();    
    let bs = std::fs::read(path.as_path()).unwrap();
    let digest = serde_json::from_slice::<crate::FragmentationDigest>(&bs).unwrap();
    let file_path = PathBuf::from(digest.name);
    
    let target_path: PathBuf = [
        dest_path, 
        file_path.strip_prefix(Path::new("/")).unwrap().to_str().unwrap()].iter().collect();
    
    create_dir_all(target_path.as_path()).await.unwrap();

    let mut target_fpath =  target_path.clone();
    target_fpath.push("file-content");
    let mut f = File::create(target_fpath.as_path()).await.unwrap();    
    let base_fpath = PathBuf::from(fragments_path);
    for i in 0..digest.fragments {
        let mut fpath = base_fpath.clone();
        fpath.push(i.to_string());        
        let bs = std::fs::read(fpath.as_path()).unwrap();
        f.write(&bs).await.unwrap();        
    }

    drop(f);    
    let crc64 =  Crc::new(target_fpath.as_path().to_str().unwrap()).checksum().unwrap().crc64;
    if crc64 == digest.crc { true }
    else {false }    
}