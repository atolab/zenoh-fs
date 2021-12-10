use std::io::Write;
use zfs::DownloadDigest;

fn write_download_digest(digest: DownloadDigest) -> std::io::Result<()>{
    let uid = uuid::Uuid::new_v4();
    if let Ok(bs) = serde_json::to_vec(&digest) {
        let fname = format!("{}/{}/{}",zfs::zfs_home(), zfs::DOWNLOAD_SUBDIR, uid.to_string());

        std::fs::write(&fname, &bs)?;
    } else {
        println!("Failed to serialise DownloadDigest -- aborting.")
    }
    Ok(())
}
fn main() {
    let mut path: String = String::new();
    let mut key: String = String::new();
    print!("Enter the zfs key of the file to download:\n:> ");
    std::io::stdout().flush().unwrap();

    match std::io::stdin().read_line(&mut key) {
        Ok(n) if n > 1 => { // Notice that this reads the \n too
            key= key[0..(n-1)].to_string();
            print!("> Enter the path where the file should be downloaded:\n:> ");
            std::io::stdout().flush().unwrap();
            path = match std::io::stdin().read_line(&mut path) {
                Ok(m) if m > 1 => {
                    path[0..(m - 1)].to_string()
                },
                _ => {
                    "./".to_string()
                }
            };

            let digest = DownloadDigest { path, key, pace: 0 };
            println!("Writing Digest:\n{:?}", &digest);
            write_download_digest(digest).unwrap();
        }
        _ => {
            println!("{} is an invalid key.", &key);
            return
        }
    }
}