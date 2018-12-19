BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
sudo docker rm --force yig
if [ -x "$BASEDIR/yig" ]; then 
    sudo docker run -d --name yig \
			 -p 8080:8080 \
	                 -p 9000:9000 \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:/work \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.18 \
			 journeymidnight/yig /work/yig
    echo "started yig from local dir"
else
    sudo docker run -d --name yig \
			 -p 8080:8080 \
	                 -p 9000:9000 \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.18 \
			 journeymidnight/yig
    echo "started yig from docker.io/journeymidnight/yig"
fi
