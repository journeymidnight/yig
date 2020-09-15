BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
WORKDIR=$1
sudo docker rm --force yig-lc
if [ -x "$BASEDIR/yig-lc" ]; then
    sudo docker run -d --name yig-lc \
			 -v ${BASEDIR}/integrate/timezone:/etc/localtime:ro \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:${WORKDIR} \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.20 \
			 journeymidnight/yig /work/yig-lc
    echo "started lc from local dir"
fi
