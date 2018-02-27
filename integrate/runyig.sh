BASEDIR=$(dirname $(pwd))
sudo docker run -it --rm --name yig \
			-p 3000:3000 \
	                 -p 9000:9000 \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/work  \
			 -v ${BASEDIR}:/var/log/yig \
                         --net=integrate_vpcbr \
			 thesues/docker-ceph-devel /work/build/bin/yig
