BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
WORKDIR=$1
sudo docker rm --force lc
if [ -x "$BASEDIR/lc" ]; then 
    sudo docker run -d --name lc \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:${WORKDIR} \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.20 \
			 journeymidnight/yig /work/lc
    echo "started lc from local dir"
fi
