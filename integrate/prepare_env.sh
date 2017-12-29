function prepare_ceph(){
	sudo docker exec ceph ceph osd pool create tiger 32
	sudo docker exec ceph ceph osd pool create rabbit 32
}


function prepare_hbase(){
	cat hbase_commands | sudo docker exec -i hbase /hbase/bin/hbase shell -n
}


prepare_ceph
prepare_hbase
