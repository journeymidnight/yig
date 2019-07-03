BASEDIR=$(dirname $(pwd))
sudo docker run --rm -v ${BASEDIR}:/work -w /work journeymidnight/yig bash -c 'make plugin_internal'