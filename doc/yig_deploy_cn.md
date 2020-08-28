# YIG部署

YIG涉及的组件包括tidb, redis, kafka,caddy, ceph, yig

### 1.关闭防火墙和selinux

```
systemctl stop firewalld
systemctl disable firewalld
setenforce 0
sed -i '/SELINUX/s/enforcing/disabled/' /etc/selinux/config
```

### 2.修改主机名

各个机器下分别执行如下命令

```
hostnamectl set-hostname  ceph117
hostnamectl set-hostname  ceph118
hostnamectl set-hostname  ceph119
```

修改后在每台机器上修改/etc/hosts文件

```
173.20.4.117 ceph117
173.20.4.118 ceph118
173.20.4.119 ceph119
```

### 3.配置master节点免密钥登陆节点

选择一台部署主机，这里选择ceph117，在root用户下开启SSH互信操作

```
在部署节点上， 使用这个命令ssh-keygen   一路回车 ，生成公钥。
然后通过命令ssh-copy-id -i  ~/.ssh/id_rsa.pub {hostname} 把公钥复制到部署需要设计的主机上。
ssh hostname测试是否成功。
```

### 4.设置时间同步

使用chrony或者ntp, 这里以chrony为例

```
yum install chrony
systemctl enable chrony
server x.x.x.x iburst    # ntp server
local  stratum 8   # 如果本机做为server
allow x.x.x.0/24   # 做为ntp server时候允许客户端访问的网络

ntpdate -u 173.20.4.117  #客户端同步server的时间
```

[chrony部署参考连接](https://www.jianshu.com/p/7b2dd5813f92)

### 5.内核参数配置确认

确认内核参数已经添加下列条目，使用“sysctl -p”即可查看，如果没有出现下列条目，就需要手动配置：

```
net.ipv4.tcp_timestamps=1
net.ipv4.tcp_keepalive_time = 1200
net.ipv4.ip_local_port_range = 1024 65000
net.ipv4.tcp_max_tw_buckets = 5000
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.tcp_syncookies = 1
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_tw_recycle = 1
net.ipv4.tcp_fin_timeout = 30
net.ipv4.conf.all.arp_ignore = 1
net.ipv4.conf.all.arp_announce = 2
net.ipv4.conf.lo.arp_ignore = 1
net.ipv4.conf.lo.arp_announce = 2
手动调整内核参数方法如下：
[root@ceph117 ~]# vi /etc/sysctl.conf  #将上述条目参数加到文件末尾
[root@ceph117 ~]# sysctl -p
```

## 部署TiDB

具体部署过程参考社区

[tidb离线部署参考连接](https://docs.pingcap.com/zh/tidb/stable/offline-deployment-using-ansible)

安装完tidb后，需要创建yig所需的数据库及表，使用mysql客户端登陆tidb，然后执行如下命令：

未安装mysql需先安装mysql

```
[root@ceph117 ~]# mysql -u root -h 173.20.4.117 -P 4000 -p
MySQL [(none)]> create database yig;
MySQL [(none)]> use yig;
MySQL [yig]> source /usr/local/yig/yig.sql;
MySQL [yig]> quit
```

## 部署Redis

### 集群规划

一般3个节点即可

### 安装redis

```
yum install redis
```

### redis服务配置

配置文件的路径为：/etc/redis.conf

#### 端口设置

选项port可以设置端口，默认6379即可

#### 关闭保护模式

设置如下选项：

```
protected-mode no
```

#### log设置

logfile 选项设置log的文件路径

#### data目录设置

选项dir用来设置redis的data目录，最好设置到高速磁盘上，比如ssd盘, data目录的owner和group要设置为redis和redis

#### 目录权限

redis的log和data目录的所有者必须设置为redis:redis
例如:设置dir 为/data1/redis

```
chown redis:redis redis
```

#### 配置slave节点信息

redis集群有master节点和slave节点，只有一个master，其余为slave，将redis配置为slave的选项如下：

```
slaveof 173.20.4.117 6379
```

#### 启动redis服务

```
systemctl enable redis
systemctl start redis
```

## 部署Kafka

kafka是以容器的方式进行部署，在部署kafka之前需要安装docker

### 安装docker

参照[docker官方文档](https://docs.docker.com/v17.12/install/)在相应的系统上安装docker

### 安装并运行zookeeper

下载zookeeper镜像到目标系统

从[dockerhub](https://hub.docker.com/_/zookeeper/)上下载zookeeper:3.5的镜像

### 创建配置文件

创建zookeeper的配置文件，在zk目录中创建conf目录，在该目录中创建zoo.cfg文件，文件内容如下：

```
dataDir=/data  
dataLogDir=/datalog  
tickTime=2000  
initLimit=5  
syncLimit=2  
autopurge.snapRetainCount=3  
autopurge.purgeInterval=0  
maxClientCnxns=60  
standaloneEnabled=true  
admin.enableServer=true  
server.1=173.20.4.117:2888:3888;2181  
server.2=173.20.4.117:2888:3888;2181  
server.3=173.20.4.117:2888:3888;2181  
```

zoo.cfg文件中的配置均为zookeeper容器里的配置，相关解释如下：

#### 设置data目录：

在目标系统里创建一个文件夹用于存放zookeeper的data文件，所创建的目录最好在高速磁盘比如ssd上。假设所创建的目录为: /data1/zk/data

#### 设置data log目录：

在目标系统里创建一个文件夹用于存放zookeeper的日志文件，所创建的目录最好在高速磁盘比如ssd上。假设所创建的目录为: /data1/zk/datalog

#### 设置当前服务器的myid:

在目录/data1/zk/data下创建文件myid，内容为当前server的id，从1开始自增，每个server的id不一样，自增即可.
 cat /data1/zk/data/myid:

```
1
```

#### 设置集群的服务器地址及端口

server.[1-3]设置服务器的地址，请根据需要设置相应的地址，端口号请不要改

#### 启动zookeeper容器

在zk目录下，运行如下命令启动zookeeper:

```
docker run -d --restart=always --net=host --name zookeeper -e "TZ=Asia/Shanghai" -v /data1/zk/conf:/zoo.cfg -v /data1/zk/data:/data -v /data1/zk/datalog:/datalog zookeeper:3.5
```

在每个目标系统上作上述设置，zookeeper要求至少有3个server，所以需要配置三台同样的server并运行相应的启动命令

#### 检查zookeeper是否成功启动

检查zookeeper容器是否运行

```
docker ps |grep zookeeper
```

如果上述命令列出zookeeper运行成功，则容器已经启动

查看zookeeper容器的日志

```
docker logs zookeeper
```

### 安装并运行kafka

下载kafka镜像到目标系统

从[dockerhub](https://hub.docker.com/r/wurstmeister/kafka/)下载kafka镜像

```
wurstmeister/kafka:2.12-2.3.0
```

### kafka配置设置

#### data目录设置

设置kafka的data目录，此目录最好设置在高速磁盘上，比如ssd盘。假设data目录为：/data1/kafka/data

#### log目录设置

设置kafka的log目录，此目录最好设置在高速磁盘上，比如ssd盘。假设log目录为：/data1/kafka/log

### 启动kafka容器

执行如下命令启动kafka容器：

```
docker run -d --name=kafka --net=host --restart=always -e "TZ=Asia/Shanghai" -e KAFKA_BROKER_ID="1" -e KAFKA_ZOOKEEPER_CONNECT="173.20.4.117:2181,173.20.4.118:2181,173.20.4.119:2181" -e KAFKA_LISTENERS="INSIDE://173.20.4.117:9092,OUTSIDE://173.20.4.117:9094" -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT" -e KAFKA_INTER_BROKER_LISTENER_NAME="INSIDE" -e KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=30000 -v /data1/kafka/data:/kafka -v /data1/kafka/logs:/opt/kafka/logs wurstmeister/kafka:2.12-2.3.0
```

在每个目标系统上作上述设置，执行过程中注意修改如下两个参数

```
KAFKA_BROKER_ID
KAFKA_LISTENERS
```

### 检查kafka容器的运行情况

#### 容器运行检查

```
docker ps|grep kafka
```

#### 查看容器运行日志

```
docker logs kafka
```

## 部署CEPH并创建存储池

### 部署ceph

具体部署方式请参考社区

### 创建存储池

请注意，无论使用哪种方式创建存储池，都需要保证有rabbit、tiger、turtle三种存储池，否则将无法使用Yig对象存储集群

创建三副本标准pool

```
[root@ceph117 ~]# ceph osd pool create rabbit 128 128
[root@ceph117 ~]# ceph osd pool create tiger 128 128
[root@ceph117 ~]# ceph osd pool create turtle 128 128
```

配置纠删码，请以root登录MON节点，进行以下操作（根据实际需要）:

```
[root@ceph117 ~]# ceph osd erasure-code-profile set fsecprofile k=2 m=1 crush-failure-domain=host crush-root={{ your_root}}
```

创建纠删码规则:

设置规则文件:

```
[root@ceph117 ~]# ceph osd crush rule create-erasure sata fsecprofile
```

创建ec pool

```
[root@ceph117 ~]# ceph osd pool create rabbit 128 128
[root@ceph117 ~]# ceph osd pool create tiger 128 128 erasure fsecprofile sata
[root@ceph117 ~]# ceph osd pool create turtle 128 128 erasure fsecprofile sata
```

## 部署Caddy

caddy安装

```
yum install caddy-1.xxx.el7.x86_64.rpm
```

caddy安装完成后按照默认配置启动

```
systemctl enable caddy
systemctl start caddy
```

[yig-front-caddy下载](https://github.com/journeymidnight/yig-front-caddy)

## 部署Yig

### 安装yig

yig[下载](https://github.com/journeymidnight/yig/releases/download/v1.3.9/yig-1.1-842.e53772dgit.el7.x86_64.rpm)

yum install yig-1.xxx.el7.x86_64.rpm

### 配置yig

参考如下配置修改/etc/yig/yig.toml

```
s3domain = ["s3.test.com", "s3-internal.test.com"]
region = "cn-bj-1"
log_path = "/var/log/yig/yig.log"
access_log_path = "/var/log/yig/access.log"
access_log_format = "{combined}"
panic_log_path = "/var/log/yig/panic.log"
log_level = "info"
pid_file = "/var/run/yig/yig.pid"
api_listener = "0.0.0.0:8080"
admin_listener = "0.0.0.0:9000"
admin_key = "secret"
ssl_key_path = ""
ssl_cert_path = ""
piggyback_update_usage = true

debug_mode = true
enable_pprof = false
pprof_listener = "0.0.0.0:8730"
reserved_origins = "s3.test.com,s3-internal.test.com"

# LC
lc_thread = 64
#lifecycle_spec = "@midnight"

# Meta Config
meta_cache_type = 2
meta_store = "tidb"
tidb_info = "root:123456@tcp(173.20.4.117:4000)/yig"
keepalive = true
enable_usage_push = false
enable_compression = false
redis_store = "single"
redis_address = "173.20.4.117:6379"
redis_group = ["ceph117:6379","ceph118:6379","ceph119:6379"]
redis_password = ""
redis_connection_number = 10
memory_cache_max_entry_count = 100000
enable_data_cache = false
redis_connect_timeout = 1
redis_read_timeout = 1
redis_write_timeout = 1
redis_keepalive = 60
redis_pool_max_idle = 3
redis_pool_idle_timeout = 30

cache_circuit_check_interval = 3
cache_circuit_close_sleep_window = 1
cache_circuit_close_required_count = 3
cache_circuit_open_threshold = 1
cache_circuit_exec_timeout = 5
cache_circuit_exec_max_concurrent = -1

db_max_open_conns = 10240
db_max_idle_conns = 1024
db_conn_max_life_seconds = 300

download_buf_pool_size = 8388608 #8MB
upload_min_chunk_size = 524288 #512KB
upload_max_chunk_size = 8388608 #8MB
big_file_threshold = 1048576 #1MB

# Migrate Config
mg_thread = 1
mg_scan_interval = 600
mg_object_cooldown = 3600

# Ceph Config
ceph_config_pattern = "/etc/ceph/*.conf"

# Plugin Config
[plugins.dummy_compression]
path = "/etc/yig/plugins/dummy_compression_plugin.so"
enable = true

[plugins.encryption_vault]
path = "/etc/yig/plugins/vault_plugin.so"
enable = false
#[plugins.encryption_vault.args]
#endpoint = "http://10.5.0.19:8200"
#kms_id = "your_id"
#kms_secret = "your_secret"
#version = 0
#keyName = "yig"


[plugins.dummy_encryption_kms]
path = "/etc/yig/plugins/dummy_kms_plugin.so"
enable = true
[plugins.dummy_encryption_kms.args]
url = "KMS"

[plugins.dummy_mq]
path = "/etc/yig/plugins/dummy_mq_plugin.so"
enable = true
[plugins.dummy_mq.args]
topic = "topic1"
url = "ceph117:9092"

[plugins.dummy_iam]
path = "/etc/yig/plugins/dummy_iam_plugin.so"
enable = true
[plugins.dummy_iam.args]
url="s3.test.com"

[plugins.not_exist]
path = "not_exist_so"
enable = false
```

### 启动yig

```
systemctl enable yig
systemctl start yig
```

### 检查所有服务状态

检查除Ceph、TiDB之外服务状态：
登录到相应节点使用systemctl status {服务名}进行查看
检查TiDB服务状态：
在TiDB节点使用mysql语句进行连接：
[root@ceph117 ~]# mysql -u root -h 173.20.4.117 -P 4000 -p
检查Ceph状态：
在ceph监控节点使用下面语句查看：
[root@ceph117 ~]# ceph -s

### 验证s3

#### 安装s3cmd

```
yum install s3cmd
```

#### 配置s3cmd

```
[root@myhost ~]# cat ~/.s3cfg
[default]
access_key = hehehehe
secret_key = hehehehe
default_mime_type = binary/octet-stream
enable_multipart = True
encoding = UTF-8
encrypt = False
host_base = s3.test.com
host_bucket = %(bucket)s.s3.test.com
use_https = False
multipart_chunk_size_mb = 5
```

使用s3cmd在Yig服务节点的机器上使用以下语句测试是否可用：

```
[root@ceph117 ~]# s3cmd ls 
[root@ceph117 ~]# s3cmd mb s3://test
[root@ceph117 ~]# touch hehe
[root@ceph117 ~]# s3cmd put hehe s3://test
[root@ceph117 ~]# s3cmd get s3://test/hehe
[root@ceph117 ~]# s3cmd del s3://test/hehe
[root@ceph117 ~]# s3cmd rb s3://test
```

