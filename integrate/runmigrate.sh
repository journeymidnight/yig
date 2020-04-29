BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
WORKDIR=$1
sudo docker rm --force migrate
if [ -x "$BASEDIR/lc" ]; then 
    sudo docker run -d --name migrate \
             -v /etc/localtime:/etc/localtime:ro \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/var/log/yig \
			 -v ${BASEDIR}:${WORKDIR} \
                         --net=integrate_vpcbr \
			 journeymidnight/yig /work/migrate
    echo "started migrate from local dir"
fi
