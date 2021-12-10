use std::io::{Read, Write};
use zfs::UploadDigest;

fn write_upload_digest(digest: UploadDigest) -> std::io::Result<()>{
    let uid = uuid::Uuid::new_v4();
    if let Ok(bs) = serde_json::to_vec(&digest) {
        let fname = format!("{}/{}/{}",zfs::zfs_home(), zfs::UPLOAD_SUBDIR, uid.to_string());
        std::fs::write(&fname, &bs)?;
    }
    Ok(())
}
fn main() {
    let mut path: String = String::new();
    let mut key: String = String::new();
    print!("Enter the path of the file to upload:\n:> ");
    std::io::stdout().flush().unwrap();
    std::io::stdin().read_to_string(&mut path).unwrap();
    if std::path::Path::new(&path).exists() {
        print!("Enter the zfs key for this file:\n:> ");
        std::io::stdout().flush().unwrap();
        match std::io::stdin().read_to_string(&mut key) {
            Ok(n) if n > 0 => {
                let digest = UploadDigest { path, key};
                write_upload_digest(digest).unwrap();
            }
            _ => {
                println ! ("{} is an invalid key.", & key);
                return
            }
        }
    } else {
        println!("The file {} does not exist.", &path);
    }
}