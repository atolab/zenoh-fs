#!/bin/bash
curl -X PUT -H 'content-type:application/json' -d '{}' http://localhost:8000/@/router/local/config/plugins/storages/backends/fs
curl -X PUT -H 'content-type:application/json' -d '{"key_expr":"/zfs/**","strip_prefix":"/zfs","dir":"zfs"}' http://localhost:8000/@/router/local/config/plugins/storages/backends/fs/storages/zfs
