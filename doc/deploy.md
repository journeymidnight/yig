# Yig deploy
	Dependences:
		1.Zookeeper
		2.HDFS
		3.HBASE
		4.CEPH
	


## Preparation
（以部署在以下3台服务器为例）
10.183.97.150，10.183.97.151，10.183.97.152

	1.JAVA intall & Env configuration
		>yum install java-1.8.0-openjdk-devel.x86_64
		向~/.bashrc添加以下几行
			export JAVA_HOME="/usr/lib/jvm/java-1.8.0-openjdk"
			export PATH=$PATH:$JAVA_HOME/bin
			export ZOOKEEPER_HOME=/letv/zookeeper/zookeeper-3.4.9
			export PATH=$ZOOKEEPER_HOME/bin:$PATH
			export HADOOP_HOME=/letv/hadoop
			export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:
			export HBASE_HOME=/letv/hbase
			export PATH=$PATH:$HBASE_HOME/bin
		
	2.计划部署Zookeeper，HDFS, HBASE的全部节点要求可以通过hostname互相（包括自己）免密ssh登录，同时添加host到/etc/hosts文件
		>cat /etc/hosts
			10.183.97.150 bj-test-yig-97-150
			10.183.97.151 bj-test-yig-97-151
			10.183.97.152 bj-test-yig-97-152
			
	3.下载安装包到/letv
		cd /letv
		wget https://mirrors.cnnic.cn/apache/zookeeper/stable/zookeeper-3.4.10.tar.gz
		wget http://mirrors.tuna.tsinghua.edu.cn/apache/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz
		wget https://mirrors.tuna.tsinghua.edu.cn/apache/hbase/1.2.6/hbase-1.2.6-bin.tar.gz
		
	
##安装Zookeeper

	
	cd /letv
	tar xvf zookeeper-3.4.9.tar.gz
	mkdir /letv/zookeeper
	mkdir /letv/zookeeper/logs
	mkdir /letv/zookeeper/zkdata
	mv zookeeper-3.4.9  zookeeper/
	cd zookeeper/zookeeper-3.4.9/conf
	mv zoo_sample.cfg zoo.cfg
	
	修改zoo.cfg，添加如下几行：
	dataDir=/letv/zookeeper/zkdata
	dataLogDir=/letv/zookeeper/logs
	server.1=bj-test-yig-97-150:2888:3888
	server.2=bj-test-yig-97-151:2888:3888
	server.3=bj-test-yig-97-152:2888:3888
	
	在每个节点创建/letv/zookeeper/zkdata/myid文件并添加对应的id如下
	echo 1 >/letv/zookeeper/zkdata   //150节点上
	echo 2 >/letv/zookeeper/zkdata   //151节点上
	echo 3 >/letv/zookeeper/zkdata   //152节点上

    在每个节点启动服务
    zkServer.sh start

通过zkCli.sh命令行判断zookeeper是否工作正常
##安装HDFS
	cd /letv
	tar xvf hadoop-2.7.3.tar.gz
	mv hadoop-2.7.3 hadoop
	cd /letv/hadoop/etc/hadoop
	
修改core-site.xml

	<configuration>
	<property>
  		<name>hadoop.tmp.dir</name>
  		<value>file:/letv/hadoop/tmp</value>
  		<description>Abase for other temporary directories.</description>
	</property>
	<property>
  		<name>fs.defaultFS</name>
  		<value>hdfs://mycluster</value>
	</property>
	<property>
   		<name>ha.zookeeper.quorum</name>
   		<value>bj-test-yig-97-150:2181,bj-test-yig-97-151:2181,bj-test-yig-97-152:2181</value>
	</property>
	</configuration>


修改hdfs-site.xml

	<configuration>
	<property>
        <name>dfs.nameservices</name>
        <value>mycluster</value>
	</property>
	<property>
        <name>dfs.ha.namenodes.mycluster</name>
        <value>bj-test-yig-97-150,bj-test-yig-97-151</value>
	</property>
	<property>
        <name>dfs.namenode.rpc-address.mycluster.bj-test-yig-97-150</name>
        <value>bj-test-yig-97-150:8020</value>
	</property>
	<property>
        <name>dfs.namenode.rpc-address.mycluster.bj-test-yig-97-151</name>
        <value>bj-test-yig-97-151:8020</value>
	</property>
	<property>
        <name>dfs.namenode.http-address.mycluster.bj-test-yig-97-150</name>
        <value>bj-test-yig-97-150:50070</value>
	</property>
	<property>
        <name>dfs.namenode.http-address.mycluster.bj-test-yig-97-151</name>
        <value>bj-test-yig-97-151:50070</value>
	</property>
	<property>
        <name>dfs.namenode.servicerpc-address.mycluster.bj-test-yig-97-150</name>
        <value>bj-test-yig-97-150:53310</value>
	</property>
	<property>
        <name>dfs.namenode.servicerpc-address.mycluster.bj-test-yig-97-151</name>
        <value>bj-test-yig-97-151:53310</value>
	</property>
	<property>
        <name>dfs.namenode.shared.edits.dir</name>
        <value>qjournal://bj-test-yig-97-150:8485;bj-test-yig-97-151:8485;bj-test-yig-97-152:8485/mycluster</value>
	</property>
	<property>       	
		<name>dfs.client.failover.proxy.provider.mycluster<name>
	<value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
	</property>
	<property>
        <name>dfs.ha.fencing.methods</name>
        <value>sshfence</value>
	</property>
	<property>
        <name>dfs.ha.fencing.ssh.private-key-files</name>
        <value>/root/.ssh/id_rsa</value>
	</property>
	<property>
        <name>dfs.journalnode.edits.dir</name>
        <value>/letv/hadoop2/journalname/data</value>
	</property>
	<property>
        <name>dfs.ha.automatic-failover.enabled</name>
        <value>true</value>
	</property>
	<property>
        <name>dfs.replication</name>
        <value>2</value>
	</property>
	<property>
    	<name>dfs.name.dir</name>
    	<value>/letv/hadoopname</value>
	</property>
	<property>
    	<name>dfs.data.dir</name>
    	<value>/data/slot0,/data/slot1,/data/slot2</value>
	</property>

	</configuration>
	
修改slave，添加启动datanode节点host

	bj-test-yig-97-150
	bj-test-yig-97-151
	bj-test-yig-97-152
	
拷贝/etv/hadoop目录到其他节点/letv下

在主namenode节点执行

	hadoop-daemons.sh start journalnode //启动journalnode集群
	hdfs zkfc -formatZK //格式化zkfc,让在zookeeper中生成ha节点
	hadoop namenode -format //格式化hdfs
	hadoop-daemon.sh start namenode //启动namenode
	
在备nomenode节点执行

	hdfs namenode -bootstrapStandby
	hadoop-daemon.sh start namenode

在主namenode节点执行

	hadoop-daemons.sh start datanode //启动datanode进程
	hadoop-daemons.sh start zkfc //启动ZKFC
	
通过http://10.183.97.150:50070/ http://10.183.97.151:50070/检查hdfs是否工作正常
	
##安装Hbase
在主namenode节点上执行

	cd /letv
	tar xvf hbase-1.2.4-bin.tar.gz
	mv hbase-1.2.4 hbase
	vim /letv/hbase/conf/hbase-env.sh
		export HBASE_PID_DIR=/letv/hbase/logs
		export HBASE_MANAGES_ZK=false
		export HADOOP_HOME=/letv/hadoop/etc/hadoop
	vim /letv/hbase/conf/hbase-site.xml
		<configuration>
  		<property>
    		<name>hbase.rootdir</name>
    		<value>hdfs://mycluster/hbase</value>
  		</property>
  		<property>
    		<name>hbase.cluster.distributed</name>
    		<value>true</value>
  		</property>
  		<property>
    		<name>hbase.zookeeper.quorum</name>
    		<value>bj-test-yig-97-150:2181,bj-test-yig-97-151:2181,bj-test-yig-97-152:2181</value>
  		</property>
		</configuration>
	vim /letv/hbase/conf/regionservers
		bj-test-yig-97-150
		bj-test-yig-97-151
		bj-test-yig-97-152
	vim /letv/hbase/conf/backup-masters
		bj-test-yig-97-151	
	ln -s /letv/hadoop/etc/hadoop/core-site.xml /letv/hbase/conf/core-site.xml
	ln -s /letv/hadoop/etc/hadoop/hdfs-site.xml /letv/hbase/conf/hdfs-site.xml
	
复制/letv/hbase 到其他安装hbase的节点

在主namenode节点上执行

	start-hbase.sh

通过http://10.183.97.150:16010 http://10.183.97.151:16010检查hbase是否工作正常

## 监控HBase


### webui监控hbase

访问:16010端口, 查看webUI


### prometheus监控hbase

HBase与Ceph不同, Ceph把数据统一收到ceph-mgr, 而HBase的话, 需要单独访问每个进程拿数据

注意:我只测试过用start\_hbase.sh和stop\_hbase.sh的情况,在所有HBase节点安装jmx_exporter_agent和配置

https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.1.0/jmx_prometheus_javaagent-0.1.0.jar


增加文件$HBASE\_HOME/conf/hbase_exporter.yml文件

	lowercaseOutputLabelNames: true
	lowercaseOutputName: true
	rules:
	  - pattern: Hadoop<service=HBase, name=RegionServer, sub=Regions><>Namespace_([^\W_]+)_table_([^\W_]+)_region_([^\W_]+)_metric_(\w+)
	    name: hbase_$4
	    labels:
	      namespace: "$1"
	      table: "$2"
	      region: "$3"
	  - pattern: Hadoop<service=HBase, name=(\w+), sub=(\w+)><>([\w.-]+)
	    name: hbase_$1_$2_$3


修改$HBASE\_HOME/bin/hbase, 增加如下, 注意修改jar和yml文件的位置

	if [ $1 == "start" ]; then
	if [ "$COMMAND" = "master" ] || [ "$COMMAND" = "regionserver" ]; then
	  for port in {7000..7010}; do
	    if [ `ss -ltpn|grep ":$port" | wc -l` == "1" ]; then
	      echo "Checking port $port - port $port in use"
	    else
	      echo "Checking port $port - port $port not in use - using port $port"
	      HBASE_OPTS="$HBASE_OPTS -javaagent:$HBASE_HOME/conf/jmx_prometheus_javaagent-0.1.0.jar=$port:$HBASE_HOME/conf/hbase_exporter.yml"
	      break
	    fi
	  done
	fi
	fi

port从7000到7010,有可能是master, 也有可能是region server
正常启动访问:7000/metrics得到监控数据



##安装YIG

安装redis

	yum install redis
	redis-server /etc/redis.conf
	
clone yig

	git clone http://github.com/journeymidnight/yig.git
	go build 
	
修改/letv/yig/conf/ 下ceph配置文件,global部分从部署ceph的机器上/etc/ceph/ceph.conf复制
client.admin部分从/etc/ceph/ceph.client.admin.keyring复制

	[global]
	fsid = f17ca0ad-70ef-4d52-afda-6a55e52bdac8
	mon_initial_members = bj-test-yig-97-151, bj-test-yig-97-152
	mon_host = 10.183.97.151,10.183.97.152
	auth_cluster_required = cephx
	auth_service_required = cephx
	auth_client_required = cephx
	filestore_xattr_use_omap = true
	enable_experimental_unrecoverable_data_corrupting_features = *

	[client.admin]
		key = AQASiRdY+eKHKxAADc595O2NCxK0KZg1E9BK6A==
		
修改/etc/yig/yig.json

	{
    "S3Domain": "yig-test.lecloudapis.com",   //部署的yig服务域名
    "Region": "cn-north-1",                   //region,现在可以随便填
    "IamEndpoint": "http://10.112.32.208:9006", //配置的IAM地址
    "IamKey": "key",                            //不允许改动
    "IamSecret": "secret",						  //不允许改动
    "LogPath": "/var/log/yig/yig.log",
    "PanicLogPath":"/var/log/yig/panic.log",
    "PidFile": "/var/run/yig/yig.pid",
    "BindApiAddress": "0.0.0.0:3000",
    "BindAdminAddress": "0.0.0.0:9000",
    "SSLKeyPath": "",
    "SSLCertPath": "",
    "ZookeeperAddress": "10.183.97.150:2181,10.183.97.151:2181,10.183.97.152:2181",
    "RedisAddress": "localhost:6379",
    "RedisConnectionNumber": 10,
    "DebugMode": true,  //该模式下iam配置不生效，仅能使用ak:hehehehe sk:hehehehe访问
    "AdminKey": "secret"
	}
	
在ceph集群上创建rabbit，tiger两个pool

在hbase节点上创建表

	sh yig/meta/create_table.sh
	
启动yig

./yig
	


	
