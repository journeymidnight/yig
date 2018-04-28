# YIG

*Y*et *a*nother *I*ndex *G*ateway 


# Introduction

## A completely new designed object storage gateway framework that fully compatible with Amazon S3

At its core, Yig extend minio backend storage to allow more than one ceph cluster work together and form a supper large storage resource pool, users could easily enlarge the pool`s capacity to EB level by adding a new ceph cluser to this pool. Benifits are avoiding data movement and IO drop down caused by adding new host or disks to old ceph cluster as usual way. To accomplish this goal, Yig need a distribute database to store meta infomation. Now already Support Tidb,MySql,Hbase.


# Getting Started

## Build

How to build?

Require:

- ceph-devel
- go(>=1.7)

Steps:

```shell
mkdir -p $GOPATH/src/github.com/journeymidnight
cd $GOPATH/src/github.com/journeymidnight
git clone git@github.com:yig/yig.git
cd $YIG_DIR
go get ./...
go build
```


build rpm package
```shell
yum install ceph-devel
sh package/rpmbuild.sh
```

## Dependency

Before running Yig, requirments below are needed:

 * Deploy at least a ceph cluster with two specify pools named 'tiger' and 'rabbit' are created. About how to deploy ceph, please refer [https://ceph.com](https://ceph.com) or our [[Sample]]()
 * Deploy a Hbase/TiDB/Mysql, then create tables. [[Sample]](https://github.com/journeymidnight/yig/blob/master/doc/deploy.md)

 	* Tidb/Mysql: 
 	
 	```
 	 MariaDB [(none)]> create database yig
 	 MariaDB [(none)]> source ../yig/integrate/yig.sql
 	```
 	
 	* Hbase

 	```
 	 sh ../yig/tools/create_table.sh
 	```
 		
 * Deploy [yig-iam](https://github.com/journeymidnight/yig-iam) used for user management and authorize request. If Yig is running in Debug Mode, request will not sent to yig-iam. So this deployment is optional, but in real factory environment, you still need it.

 * Deploy a standalone Redis instance used as cache for better performance. This deployment is optional but strong recommend

 ```
 yum install redis
 ```

## Config files

Main config file of Yig is located at ```/etc/yig/yig.json ``` by default

```
{
    "S3Domain": "s3.test.com",
    "Region": "cn-bj-1",
    "IamEndpoint": "http://10.11.144.11:9006",
    "IamKey": "key",
    "IamSecret": "secret",
    "LogPath": "/var/log/yig/yig.log",
    "PanicLogPath":"/var/log/yig/panic.log",
    "PidFile": "/var/run/yig/yig.pid",
    "BindApiAddress": "0.0.0.0:3000",
    "BindAdminAddress": "0.0.0.0:9000",
    "SSLKeyPath": "",
    "SSLCertPath": "",
    "ZookeeperAddress": "10.110.95.56:2181,10.110.95.62:2181",
    "EnableCache": true,
    "RedisAddress": "localhost:6379",
    "RedisConnectionNumber": 10,
    "InMemoryCacheMaxEntryCount": 100000,
    "DebugMode": false,
    "AdminKey": "secret",
    "MetaCacheType": 2,
    "EnableDataCache": true,
    "CephConfigPattern": "/etc/yig/conf/*.conf",
    "GcThread": 1,
    "LogLevel": 5,
    "ReservedOrigins":"sample.abc.com",
    "TidbInfo"
}
```

### Meanings of options above:

```
S3Domain: your s3 service domain
Region: doesn`t matter
IamEndpoint: address of iam service
IamKey: specify as your wish, but must be the same as iam config files
IamSecret: specify as your wish, but must be the same as iam config files
LogPath: location of yig access log file
PanicLogPath: location of yig panic log file
BindApiAddress: your s3 service endpoint
BindAdminAddress: end point for tools/admin
SSLKeyPath: SSL key location 
SSLCertPath: SSL Cert location
ZookeeperAddress: zookeeper address if you choose hbase
EnableCache: switch of cache
RedisAddress: Redis access address
DebugMode: if this is set true, only requestes signed by [AK/SK:hehehehe/hehehehe] are valid
AdminKey: used for tools/admin
MetaCacheType: 
EnableDataCache:
CephConfigPattern: ceph config files for yig
GcThread: control gc speed when tools/lc is running
LogLevel: [1-20] the bigger number is, the more log output to log file
ReservedOrigins: set CORS when s3 request are from web browser
TidbInfo:

```

Ceph config files

Combine your ceph cluster config file [/etc/ceph/ceph.conf] with [/etc/ceph/ceph.client.admin.keyring] together, then put it to the location which 'CephConfigPattern' specified, a sample is below

```
[global]
fsid = 7b3c9d3a-65f3-4024-aaf1-a29b9422665c
mon_initial_members = ceph57
mon_host = 10.180.92.57
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx
filestore_xattr_use_omap = true
osd pool default size = 3
osd pool default min size = 2
osd pool default pg num = 128
osd pool default pgp num = 128

[client.admin]
        key = AQCulvxWKAl/MRAA0weYOmmkArUm/CGBHX0eSA==

```


## Run

Start server:
```shell
cd $YIG_DIR
sudo ./yig
```

OR 

```
systemctl start yig
```

 
## Documentation

Please refer our wiki
 


