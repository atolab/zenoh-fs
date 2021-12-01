#!/bin/bash
curl -X PUT -H 'content-type:application/properties' http://localhost:8000/@/router/local/plugin/storages/backend/fs
curl -X PUT -H 'content-type:application/properties' -d \"path_expr=/zfs/**;path_prefix=/zfs;dir=zfs\" http://localhost:8000/@/router/local/plugin/storages/backend/fs/storage/zfs