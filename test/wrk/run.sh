#!/usr/bin/env bash

echo "RUN luarocks install luaossl FIRST"
#based on previous test, 3 osd disks could have [1000 ops/s for 4K bytes], [800 ops/s for 127K bytes]
wrk -c200 -d10s -t 5 --timeout 10s -s put.lua http://yig-test.lecloudapis.com:3000
