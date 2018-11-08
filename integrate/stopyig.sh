#!/bin/bash

sudo docker ps|while read l 
do image=`echo $l|awk '{print $2}'`
	if [[ "$image" = "yig" ]]; then 
		cid=`echo $l|awk '{print $1}'`
		docker stop $cid
	fi  
done

