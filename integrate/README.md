
Use docker-compose to start a yig env

# Prequire

+ docker-compose



# Arch

+ ceph/demo //ceph is jewel on ubuntu
+ hbase
+ redis
+ yig


# Usage


## Setup yig runtime env

```
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



