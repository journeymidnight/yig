BASEDIR=$(dirname $(pwd))
sudo docker run --rm -ti -p 3000:3000 -p 9000:9000  -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ --net=integrate_vpcbr -v ${BASEDIR}/conf/:/etc/yig/ -v ${BASEDIR}:/work  -v ${BASEDIR}:/var/log/yig -w /work thesues/docker-ceph-devel bash
