use clap::{App, Arg};
use zfs::{zfs_upload_digest_dir, UploadDigest};

fn write_upload_digest(digest: UploadDigest) -> std::io::Result<()> {
    let uid = uuid::Uuid::new_v4();
    let fname = format!("{}/{}", zfs_upload_digest_dir(), uid);
    if let Ok(bs) = serde_json::to_vec(&digest) {
        std::fs::write(&fname, &bs)?;
    }
    Ok(())
}

fn parse_args() -> (String, String, usize) {
    let args = App::new("zut: zfs utility to upload files.")
        .arg(
            Arg::from_usage("-p, --path[PATH]...  'The path for the file to upload.'")
                .required(true),
        )
        .arg(
            Arg::from_usage(
                "-k, --key=[KEY]...  'The key under which this file will be stored in zfs.'",
            )
            .required(true),
        )
        .arg(
            Arg::from_usage(
                "-f, --fragment=[BYTES] 'The size of the fragment'",
            ).default_value("32768")
        )
        .get_matches();

    (
        args.value_of("path").unwrap().to_string(),
        args.value_of("key").unwrap().to_string(),
        args.value_of("fragment").unwrap().parse().unwrap()
    )
}
fn main() {
    let (path, key, fragment_size) = parse_args();
    if std::path::Path::new(&path).exists() {
        let digest = UploadDigest { path, key, fragment_size };
        write_upload_digest(digest).unwrap();
    } else {
        println!("The file {} does not exit", &path);
    }
}
