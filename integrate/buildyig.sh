BASEDIR=$(dirname $(pwd))
TARGET=$1
sudo docker run --rm -v ${BASEDIR}:/work -w /work journeymidnight/yig bash -c "make build${TARGET}_internal"
