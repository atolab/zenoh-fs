# zenoh-fs
A zenoh based distributed file system supporting extremely large data files. 
Specifically, zenoh-fs provides a series of utilities as part of the ```zfs``` crate 
to fragment and reassembly large files as well as to upload and download from zenoh 
storages.

```zenoh-fs``` also provides a small deamon, namely ```zfsd``` that allows you to automatically
upload/download later files from zenoh storages.


## Getting Started
### Prerequisities
In what follows we will assume that you have a zenoh infrastructure up and running. 
If you want to test on your standalone machine, the simplest way is to start a 
```zenoh``` router and enable the loading of the storage plugin. 

To install zenoh follow the instructions available [here](https://github.com/eclipse-zenoh/zenoh).
You'll also need to install the filesystem back-end which is available [here](https://github.com/eclipse-zenoh/zenoh-backend-filesystem)

Make sure that all plugin libraries are available under:

    ~/.zenoh/lib

Then simply start the zenoh router using the following command:

    $ /path/to/zenoh/zenohd -c zenoh.json5

This command will setup the proper storage filesystem. 

## Building ZFS
In order to build zfs you need to install rust. To do so please follow the \
details provided [here](https://www.rust-lang.org/tools/install).

Once installed ```rust``` then do:

    zenoh-fs$ cargo build --release --all

### Starting zfsd
Assuming you have compiled from sources  then simply do:

    zenoh-fs$ export ZFS_HOME=$HOME/.zenoh/zenoh_backend_fs/zfs
    zenoh-fs$ ./target/release/zfsd  

### Uploading a file 
To upload a file use the `zut` utility as follows:

    zenoh-fs$ export ZFS_HOME=$HOME/.zenoh/zenoh_backend_fs/zfs
    zenoh-fs$ ./target/release/zut -k test/zut -p ./target/release/zut

This command is uploading the file `./target/release/zut` into the `zfsd`. 

### Downloading a file
To download a file use the `zet` utility as follows:

    zenoh-fs$ export ZFS_HOME=$HOME/.zenoh/zenoh_backend_fs/zfs
    zenoh-fs$ ./target/release/zet -k test/zut -p ./zut2

This command will provision the download of `test/zut` and will de-fragment and save it as
`./zut2` once done. 

At this point, to verify that all went fine do:

    zenoh-fs$ chmod +x ./zut2
    zenoh-fs$ ./zut2 -h
    zut: zfs utility to upload files.
    
    USAGE:
    zut [OPTIONS] --key <KEY>... --path <PATH>...
    
    FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information
    
    OPTIONS:
    -f, --fragment <BYTES>    The size of the fragment [default: 32768]
    -k, --key <KEY>...        The key under which this file will be stored in zfs.
    -p, --path <PATH>...      The path for the file to upload.

## Basic Deployment
You can try this locally with a single zenoh router. Or else you can start a zenoh route on one machine, start 
two `zfsd` on two different machines and then use `zut` and `zet` to upload and download files.

