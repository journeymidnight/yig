
Use docker-compose to start a yig env

# Require
+ docker
+ docker-compose

# Arch

+ ceph/demo //ceph is jewel on ubuntu
+ TiDB
+ redis
+ yig

# Usage

## Setup docker && docker-compose

```
yum install docker -y
pip install docker-compose --ignore-installed requests
```

## Build docker image of yig

```
cd ${YIGDIR}
make image
```

## Setup yig runtime env

```
cd ${YIGDIR}/intergrate
make env
```

when ceph/hbase/redis is ready


```
make prepare
```

This would create hbase table and create ceph pools


## Build yig


```
sh buildyig.sh
make
```


## Run yig

```
sh runyig.sh
```



