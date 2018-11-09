
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

Add configs to conf file:
```
#严格按照resolv-file文件中的顺序从上到下进行DNS解析，直到第一个解析成功为止
strict-order

#定义dnsmasq监听的地址，默认是监控本机的所有网卡上。局域网内主机若要使用dnsmasq服务时，指定本机的IP地址
listen-address=127.0.0.1

#忽略/etc/hosts
no-hosts
```

**Warning**:

- If Linux, add config in file:
```
address=/.s3.test.com/10.5.0.18
```
**PS: 10.5.0.18 is the IP addresses for the yig container**

- If Mac OS, add config in file:
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




