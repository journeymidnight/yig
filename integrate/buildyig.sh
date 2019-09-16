BASEDIR=$(dirname $(pwd))
BUILDDIR=$1
sudo docker run --rm -v ${BASEDIR}:${BUILDDIR} -w ${BUILDDIR} journeymidnight/yig bash -c 'make build_internal'
