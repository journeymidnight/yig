# YIG-DEPLOY

## Dependency and Require

tidb, redis, kafka,caddy, ceph, yig

## Prepare

### 1.Close firewalld and Disable SELinux

```
systemctl stop firewalld
systemctl disable firewalld
setenforce 0
sed -i '/SELINUX/s/enforcing/disabled/' /etc/selinux/config
```

### 2.Configure Hosts File

Run the command below to change the hostname.

```
hostnamectl set-hostname  ceph117
hostnamectl set-hostname  ceph118
hostnamectl set-hostname  ceph119
```

Edit the /etc/hosts file on all node with the vim editor and add lines with the IP address and hostnames of all cluster nodes.

```
173.20.4.117 ceph117
173.20.4.118 ceph118
173.20.4.119 ceph119
```

### 3.Configure the SSH Server

Configure ceph117 for an SSH passwordless login to other nodes. Execute the following commands from ceph117:

```markup
ssh-keygen
```

Now add the SSH key to all nodes with the ssh-copy-id command.

```
ssh-copy-id -i  ~/.ssh/id_rsa.pub {hostname}
```

### 4.Install and Configure NTP

Use chrony or NTP, for example , chrony

Configure chrony, the configuration file of chrony is located at **/etc/chrony.conf**

```
yum install chrony
systemctl enable chrony
server x.x.x.x iburst    # ntp server
local  stratum 8   
allow x.x.x.0/24   

ntpdate -u 173.20.4.117  #update time
```

### 5. Configuration Kernel parameter

Modifying the configuration file /etc/sysctl.conf,You need to add the following line to /etc/sysctl.conf

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
Edit the sysctl.conf file with vim.
[root@ceph117 ~]# vi /etc/sysctl.conf  
[root@ceph117 ~]# sysctl -p
```

## TiDB

Refer to the TIDB community [offline-deployment](https://docs.pingcap.com/tidb/stable/offline-deployment-using-ansible)

Connect to the TiDB cluster using the MySQL client.Create database yig and table

```
[root@ceph117 ~]# mysql -u root -h 173.20.4.117 -P 4000 -p
MySQL [(none)]> create database yig;
MySQL [(none)]> use yig;
MySQL [yig]> source /usr/local/yig/yig.sql;
MySQL [yig]> quit
```

## Redis

## Install Redis

```
yum install redis
```

## Configure Redis

The default configuration for **Redis** is **/etc/redis.conf**

#### Close Protected-Mod

```
protected-mode no
```

#### Configure the Slave Linode

Configure a slave instance by adding the `slaveof` directive into `redis.conf` to setup the replication：

```
slaveof 173.20.4.117 6379
```

#### Start Redis

```
systemctl enable redis
systemctl start redis
```

## Kafka

### Install Docker

Refer to [docker](https://docs.docker.com/v17.12/install/)

## Run zookeeper

Download ZooKeeper from [dockerhub](https://hub.docker.com/_/zookeeper/)

## Configure Zookeeper

Create the ZooKeeper configuration properties file  "zoo.cfg" in /data1/zk/conf. A multi-node setup does require a few additional configurations：

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

ZooKeeper does not require configuration tuning for most deployments. Below are a few important parameters to consider：

#### dataDir：

The directory where ZooKeeper in-memory database snapshots and, unless specified in `dataLogDir`, the transaction log of updates to the database. This location should be a dedicated disk that is ideally an SSD. Example:

```
/data1/zk/data
```

#### dataLogDir：

The location where the transaction log is written to. If you don’t specify this option, the log is written to dataDir. By specifying this option, you can use a dedicated log device, and help avoid competition between logging and snapshots.Example:

```
/data1/zk/datalog
```

#### myid:

`myid` is the server identification number. In this example, there are three servers, so each one will have a different `myid` with values `1`, `2`, and `3` respectively. The `myid` is set by creating a file named `myid` in the `dataDir` that contains a single integer in human readable ASCII text. This value must match one of the `myid` values from the configuration file. If another ensemble member has already been started with a conflicting `myid` value, an error will be thrown upon startup.
 cat /data1/zk/data/myid:

```
1
```

#### Start ZooKeeper

Creating the zookeeper image from the Dockerfile.

```
docker run -d --restart=always --net=host --name zookeeper -e "TZ=Asia/Shanghai" -v /data1/zk/conf:/zoo.cfg -v /data1/zk/data:/data -v /data1/zk/datalog:/datalog zookeeper:3.5
```

make sure the services are up and running

```
docker ps |grep zookeeper
```

Check the ZooKeeper logs to verify that ZooKeeper is healthy.

```
docker logs zookeeper
```

### Run Kafka

Download ZooKeeper from [dockerhub](https://hub.docker.com/r/wurstmeister/kafka/)

```
wurstmeister/kafka:2.12-2.3.0
```

### Configure Kafka

#### data

This location should be a dedicated disk that is ideally an SSD. Example:

```
/data1/kafka/data
```

#### log

This location should be a dedicated disk that is ideally an SSD. Example:

```
/data1/kafka/log
```

### Start Kafka

Creating the zookeeperimage from the Dockerfile:

```
docker run -d --name=kafka --net=host --restart=always -e "TZ=Asia/Shanghai" -e KAFKA_BROKER_ID="1" -e KAFKA_ZOOKEEPER_CONNECT="173.20.4.117:2181,173.20.4.118:2181,173.20.4.119:2181" -e KAFKA_LISTENERS="INSIDE://173.20.4.117:9092,OUTSIDE://173.20.4.117:9094" -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT" -e KAFKA_INTER_BROKER_LISTENER_NAME="INSIDE" -e KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=30000 -v /data1/kafka/data:/kafka -v /data1/kafka/logs:/opt/kafka/logs wurstmeister/kafka:2.12-2.3.0
```

The broker id for this server. If unset, a unique broker id will be generated.To avoid conflicts between zookeeper generated broker id's and user configured broker id's, generated broker ids start from reserved.broker.max.id + 1.

make sure the services are up and running

```
docker ps|grep kafka
```

Check the ZooKeeper logs to verify that ZooKeeper is healthy. For example, for service ceph117:

```
docker logs kafka
```

## ceph deploy

### deploy ceph

Refer to the Ceph community

### Create Pools

Note that either way, you need to make sure you have Rabbit, Tiger, and Turtle pools, or you won't be able to use the Yig object storage cluster.

create replicated pool

```
[root@ceph117 ~]# ceph osd pool create rabbit 128 128
[root@ceph117 ~]# ceph osd pool create tiger 128 128
[root@ceph117 ~]# ceph osd pool create turtle 128 128
```

create a new erasure code profile::

```
[root@ceph117 ~]# ceph osd erasure-code-profile set fsecprofile k=2 m=1 crush-failure-domain=host crush-root={{ your_root}}
```

create erasure rule:

```
[root@ceph117 ~]# ceph osd crush rule create-erasure sata fsecprofile
```

create ec pool

```
[root@ceph117 ~]# ceph osd pool create rabbit 128 128
[root@ceph117 ~]# ceph osd pool create tiger 128 128 erasure fsecprofile sata
[root@ceph117 ~]# ceph osd pool create turtle 128 128 erasure fsecprofile sata
```

## Caddy

### install caddy

```
yum install caddy-1.xxx.el7.x86_64.rpm
```

start caddy by default configuration

```
systemctl enable caddy
systemctl start caddy
```

Download Caddy from [yig-front-caddy](https://github.com/journeymidnight/yig-front-caddy)

## Yig

### install yig

Download yig from [yig](https://github.com/journeymidnight/yig/releases/download/v1.3.9/yig-1.1-842.e53772dgit.el7.x86_64.rpm)

yum install yig-1.xxx.el7.x86_64.rpm

### Configure yig

Change /etc/yig/yig.toml

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

### Start Yig.

```
systemctl enable yig
systemctl start yig
```

### Verify your yig with s3cmd

#### Install s3cmd

```
yum install s3cmd
```

#### Add a configure file under ~/.s3cfg

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

Connect to your yig and test：

```
[root@ceph117 ~]# s3cmd ls 
[root@ceph117 ~]# s3cmd mb s3://test
[root@ceph117 ~]# touch hehe
[root@ceph117 ~]# s3cmd put hehe s3://test
[root@ceph117 ~]# s3cmd get s3://test/hehe
[root@ceph117 ~]# s3cmd del s3://test/hehe
[root@ceph117 ~]# s3cmd rb s3://test
```

