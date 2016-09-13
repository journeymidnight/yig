#!/bin/sh

# Script to clear HBase tables for tests
# Refer to http://wiki.letv.cn/display/pla/HBase+Table+Schema for more information

exec hbase shell <<EOF
truncate 'buckets'

truncate 'objects'

truncate 'users'

truncate 'multiparts'

truncate 'garbageCollection'
EOF