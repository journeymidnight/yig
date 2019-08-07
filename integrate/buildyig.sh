BASEDIR=$(dirname $(pwd))
sudo docker run --rm -v ${BASEDIR}:/work -w /work journeymidnight/yig bash -c 'export GOPROXY=https://goproxy.cn;make build_internal'
