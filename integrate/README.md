
Use docker-compose to start a yig env

# Require
+ docker
+ docker-compose
+ dnsmasq

# Arch

+ ceph/demo //ceph is jewel on ubuntu
+ TiDB
+ redis
+ yig

# Usage

## Setup Requires
- Linux
```
yum install docker -y
pip install docker-compose --ignore-installed requests
yum install dnsmasq -y
```

- MAC OS

Download And Install Docker Desktop
https://download.docker.com/mac/stable/Docker.dmg


Download And Install Docker Toolbox(optional):
https://download.docker.com/mac/stable/DockerToolbox.pkg


Other details see: https://docs.docker.com/docker-for-mac/

then:
```
brew install dnsmasq
```

## Dnsmasq
#### Modify Config
- Linux
```
sudo vim /etc/dnsmasq.conf
```

- MAC OS:
```
sudo cp /usr/local/opt/dnsmasq/dnsmasq.conf.example /usr/local/etc/dnsmasq.conf
sudo vim /usr/local/etc/dnsmasq.conf
```

**Warning**:

- Add config in file:
```
address=/.s3.test.com/127.0.0.1
```
See details: https://docs.docker.com/docker-for-mac/networking/#known-limitations-use-cases-and-workarounds

#### Base Commannd

- Linux
```
systemctl enable dnsmasq
systemctl start dnsmasq
systemctl restart dnsmasq
#查看dnsmasq是否启动正常，查看系统日志：
journalctl -u dnsmasq
```

- Mac OS

```
#启动
sudo brew services start dnsmasq
#重启
sudo brew services restart dnsmasq
#停止
sudo brew services stop dnsmasq
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
cd ${YIGDIR}
make build
```


## Run yig
```
cd ${YIGDIR}
make run
```
# Test yig
## Install s3cmd
Reference: [https://github.com/s3tools/s3cmd/blob/master/INSTALL](https://github.com/s3tools/s3cmd/blob/master/INSTALL)

## Add file ~/.s3cfg 
```
[default]
access_key = hehehehe
secret_key = hehehehe
default_mime_type = binary/octet-stream
enable_multipart = True
encoding = UTF-8
encrypt = False
use_https = False
host_base = s3.test.com:8080
host_bucket = %(bucket)s.s3.test.com:8080
multipart_chunk_size_mb = 128
```

## Run s3cmd


