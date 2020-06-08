#!/bin/bash
sleep 10
for ((i=1;i<2;))
do
a=`lsof -i:8080 | wc -l`
if [ $a -eq 0 ];then
    echo "No concurrent build pipeline, you can start building"
    i=$i+1
else
    echo "There are already pipelines under construction, wait for 20s to check again until the other pipelines are completed"
    sleep 20
    i=1
fi
done
