#!/bin/bash

find ./plugins/files -name "*.go" | while read gofile ;
do
    file=`echo ${gofile##*/}`
    filename=`echo ${file%.*}`
    go build -buildmode=plugin -o ./plugins/${filename}.so ${gofile}
done