use clap::{App, Arg};
use zfs::DownloadDigest;

fn write_download_digest(digest: DownloadDigest) -> std::io::Result<()> {
    let uid = uuid::Uuid::new_v4();
    if let Ok(bs) = serde_json::to_vec(&digest) {
        let fname = format!(
            "{}/{}/{}",
            zfs::zfs_home(),
            zfs::DOWNLOAD_SUBDIR,
            uid.to_string()
        );

        std::fs::write(&fname, &bs)?;
    } else {
        println!("Failed to serialise DownloadDigest -- aborting.")
    }
    Ok(())
}
fn parse_args() -> (String, String) {
    let args = App::new("zut: zfs utility to upload files.")
        .arg(
            Arg::from_usage("-p, --path[PATH]...  'The path for the file to upload.'")
                .required(true),
        )
        .arg(
            Arg::from_usage(
                "-k, --key=[KEY]...  'The key under which this file will be stored in zfs'",
            )
            .required(true),
        )
        .get_matches();

    (
        args.value_of("path").unwrap().to_string(),
        args.value_of("key").unwrap().to_string(),
    )
}

fn main() {
    let (path, key) = parse_args();
    let digest = DownloadDigest { path, key, pace: 0 };
    write_download_digest(digest).unwrap();
}
