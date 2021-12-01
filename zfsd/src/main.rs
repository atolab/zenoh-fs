use zfs;
use clap::{App, Arg};
use notify::{Watcher, RecursiveMode, DebouncedEvent};
use core::panic;
use std::{sync::mpsc::channel, time::Duration};
use std::path::{PathBuf};
use async_std::fs::{create_dir_all};
use zenoh;
use zenoh::Properties;
use futures::prelude::*;
use zenoh::net::*;

const EVT_DELAY: u64 = 1;
const DOWNLOAD_SUBDIR: &str = "download";
const UPLOAD_SUBDIR: &str = "upload";
const FRAGS_SUBDIR: &str = "frags";
const FRAGMENT_SIZE: usize = 4*1024;


// async fn store() {
//     let args: Vec<String> = std::env::args().into_iter().collect();
//     if args.len() < 4 {
//         println!("Usage:\n\tzfs-write <file-path> <fragments-path> <key> [<fragment-size>]");
//         return;
//     } 
//     let fragment_size = if args.len() >= 5 {
//         match args[4].parse() {
//             Ok(n) => n,
//             _ => {
//                 println!("Invalid fragment size: {}", args[4]);
//                 return
//             }
//         }
//     } else { zfs::FRAGMENT_SIZE };

//     let d = zfs::frag::fragment(&args[1], &args[2], &args[3], fragment_size).await.unwrap();
//     println!("Done with fragmentation with digest: \n {:?}", d)  
// }

// async fn get() {
//     let args: Vec<String> = std::env::args().into_iter().collect();
//     if args.len() < 3 {
//         println!("Usage:\n\tzfs-read <fragments-path> <defragment-path>");
//         return;
//     } 


//     let r = zfs::frag::defragment(&args[1], &args[2]).await;
//     if r == true {
//         println!("Successful defragmentation!");
//     } else {
//         println!("Unsuccessful defragmentation!");
//     }
    
// }

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
        config.insert("remote-endpoints".to_string(), values.collect::<Vec<&str>>().join(","));
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

    // // let upld  = String::from(upld_dir.to_str().unwrap());
    // // let dwnld  = String::from(dwnld_dir.to_str().unwrap());
    // Ok((upld, dwnld))
    Ok(())
    
}

async fn fragment(path: &str, fragment_size: usize) -> Result<(), String> {
    let path = PathBuf::from(path);        
    let mut target = PathBuf::from(path.parent().unwrap());
    target.push(FRAGS_SUBDIR);    

    let bs = std::fs::read(path.as_path()).unwrap();    
    let upload_spec  = match serde_json::from_slice::<zfs::UploadDigest>(&bs) {
        Ok(us) => us,
        Err(e) => {
            println!("Serde failed with {:?}",e);
            panic!();
        }
    };
    println!("Uploading: {} as {}", &upload_spec.path, &upload_spec.key);
    zfs::frag::fragment(
        &upload_spec.path, 
        target.to_str().unwrap(), 
        &upload_spec.key,
        fragment_size).await.unwrap();
    Ok(())
}
async fn upload_fragment(z: &Session, path: &str, key: &str) {
    let path = PathBuf::from(path);            
    let bs = std::fs::read(path.as_path()).unwrap();    
    z.write(&key.into(), bs.into()).await.unwrap();
}
async fn download(z: &Session, key: &str, target_path: &str) -> Result<(), String> {
    let manifest = format!("{}/{}",key, zfs::ZFS_DIGEST);
    
    let mut replies = z.query(
        &manifest.into(),
        "",
        QueryTarget {
            kind: queryable::STORAGE,
            target: Target::default(),
        },
        QueryConsolidation::default(),
    ).await.unwrap();
    
    if let Some(reply) = replies.next().await {
        let bs = reply.data.payload.contiguous();
        let digest = serde_json::from_slice::<zfs::FragmentationDigest>(&bs).unwrap();
        for i in 0..digest.fragments {
            let path = format!("{}/{}", key, i);
            
        }
    }
    Ok(())
}

#[async_std::main]
async fn main() {     
    let (path, zconf) = parse_args();    
    let z = open(zconf.into()).await.unwrap();
    let _ = init(&path).await.unwrap();
    let (tx, rx) = channel();
    let mut watcher = notify::watcher(tx, Duration::from_secs(EVT_DELAY)).unwrap();
    let fragment_size = FRAGMENT_SIZE;
    let _  = watcher.watch(&path, RecursiveMode::Recursive);   
    let mut frags_dir = PathBuf::from(path);
    frags_dir.push(UPLOAD_SUBDIR);
    frags_dir.push(FRAGS_SUBDIR);     

    while let Ok(evt) = rx.recv() {
        match evt {
            DebouncedEvent::Create(path) => {
                if path.is_file() {
                    let parent = path.parent().unwrap();
                    
                    if parent.ends_with(DOWNLOAD_SUBDIR) {
                        println!("Downloading {:?}", &path);

                    } else if parent.ends_with(UPLOAD_SUBDIR) {
                        println!("Fragmenting {:?}", &path);
                        fragment(path.to_str().unwrap(), fragment_size).await.unwrap()
                    }                     
                    else {
                        let fpath = path.to_str().unwrap();
                        match fpath.find(FRAGS_SUBDIR) {
                            Some(_) => {                    
                                let key = fpath.strip_prefix(frags_dir.to_str().unwrap()).unwrap();
                                println!("Uploading fragment : {:?} as {:?}", path, key);
                                upload_fragment(&z, fpath, key).await;
                                println!("Completed  upload of: {:?} as {:?}", path, key);
                            },
                            None => {
                                println!("Ignoring {:?} path...", &path)
                            }

                        }
                    }
                }

            }
            _ => {}
        }

    }
}