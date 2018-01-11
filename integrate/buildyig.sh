BASEDIR=$(dirname $(pwd))
sudo docker run --rm -v ${BASEDIR}:/work -w /work thesues/docker-ceph-devel bash -c 'make'
