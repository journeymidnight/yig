#!/usr/bin/env bash

echo "RUN luarocks install luaossl FIRST'
wrk -c64 -d10s --timeout 10s -s put.lua http://yig-test.lecloudapis.com:3000
