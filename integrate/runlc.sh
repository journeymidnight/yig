BASEDIR=$(dirname $(pwd))
echo ${BASEDIR}
sed -i '/DebugMode/a lcdebug = true' $(pwd)/yigconf/yig.toml
sudo docker rm --force lc
if [ -x "$BASEDIR/lc" ]; then 
    sudo docker run -d --name lc \
			 -v ${BASEDIR}/integrate/cephconf:/etc/ceph/ \
			 -v ${BASEDIR}/integrate/yigconf:/etc/yig/ \
			 -v ${BASEDIR}:/work \
                         --net=integrate_vpcbr \
                         --ip 10.5.0.20 \
			 journeymidnight/yig /work/lc
    echo "started lc from local dir"
fi
