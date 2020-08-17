BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
WORKDIR=$1
sudo docker rm --force restore
if [ -x "$BASEDIR/yig-restore" ]; then
    sudo docker run -d --name restore \
             -v ${BASEDIR}/integrate/timezone:/etc/localtime:ro \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:${WORKDIR} \
                         --net=integrate_vpcbr \
			 journeymidnight/yig /work/yig-restore
    echo "started yig-restore from local dir"
fi
