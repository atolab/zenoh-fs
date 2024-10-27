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

    zenoh-fs$ ./target/release/zfsd # assuming you are OK using ~/.zfs as home 

### Uploading a file 
Let's upload the ```zfsd``` executable, in this case you need to create a 
manifest file containing the following content:

    { 
        "path": "/some/path/zenoh-fs/target/target/zfsd",
        "key": "/zfs/bin/zfsd"
    }

Let's save this file as ```upload-zfsd``` and then just copy it to ```~/.zfs/upload```.
That's it, ```zenoh-fs``` will take care of fragmenting it and uploading on the 
distributed storage.

### Downloading a file
Let's assume that now we want to get the ```zfsd``` off the zenoh storage network.
To achieve this, simply create a manifest including the following information:

    {
        "key": "/zfs/bin/zfsd",
        "path": "/some-path/zfsd",
        "pace": 0
    }

Then save this file as ```download-zfsd``` and copy it under ```~/.zfs/download```,
 et voilà your file will be automatically downloaded, reassembled and stored at the 
requested ```path``` (as per the manifest).

Once the download complites, do:

    $ chmod +x /some-path/zfsd
    $ /some-path/zfsd --help

You should see the help for ```zfsd``` demonstrating that everything worked like a charm.
    
