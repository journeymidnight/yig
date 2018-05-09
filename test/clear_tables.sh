#!/bin/sh

# Script to clear HBase tables for tests

exec hbase shell <<EOF
truncate 'buckets'

truncate 'objects'

truncate 'users'

truncate 'multiparts'

truncate 'garbageCollection'
EOF
