
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
cd ${YIGDIR}
make env
```

This would create tidb table and create ceph pools

## Build yig
```
sh buildyig.sh
```


## Run yig
```
sh runyig.sh
```

## Stop yig
```
sh stopyig.sh
```




