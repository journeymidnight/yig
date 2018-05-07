BASEDIR=$(dirname $(pwd))
sudo docker run -d  --name yig \
			-p 80:80 \
	                 -p 9000:9000 \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.18 \
			 yig /work/build/bin/yig
