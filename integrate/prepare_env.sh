function prepare_ceph(){
	sudo docker exec ceph ceph osd pool create tiger 32
	sudo docker exec ceph ceph osd pool create rabbit 32
}


function prepare_hbase(){
	cat hbase_commands | sudo docker exec -i hbase /hbase/bin/hbase shell -n
}

function prepare_mysql(){
	docker exec -i mysql mysql -e "create database yig character set utf8;"
	mysql -h 10.5.0.9 yig<yig.sql
}

prepare_ceph
prepare_hbase
prepare_mysql
