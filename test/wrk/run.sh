#!/usr/bin/env bash

wrk -c100 -t10 --timeout 10 -d1h -s put.lua http://10.75.144.240:3000