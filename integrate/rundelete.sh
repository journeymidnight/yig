BASEDIR=$(dirname $(pwd))
sudo docker run -d   --name delete \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:/work \
			 --net=integrate_vpcbr \
			 journeymidnight/yig /work/build/bin/delete
