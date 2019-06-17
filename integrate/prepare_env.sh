function prepare_ceph(){
	docker exec ceph ceph osd pool create tiger 32
	docker exec ceph ceph osd pool create rabbit 32
}

function prepare_hbase(){
	cat hbase_commands | sudo docker exec -i hbase /hbase/bin/hbase shell -n
}

function prepare_mysql(){
	docker exec mysql mysql -e "create database yig character set utf8;"
    docker exec mysql mysql -e "use yig;source /yig.sql;"
}

function prepare_tidb(){
	docker exec mysql mysql -P 4000 -h 10.5.0.17 -e "create database yig character set utf8;"
    docker exec mysql mysql -P 4000 -h 10.5.0.17 -e "use yig;source /yig.sql;"
}

function prepare_vault(){
    echo "start init vault transit..."
    docker exec vault vault secrets enable transit
    docker exec vault vault write -f transit/keys/yig
}

echo "creating Ceph pool..."
prepare_ceph
echo "creating  HBase table..."
#prepare_hbase
echo "creating  MySQL db..."
docker cp yig.sql mysql:/yig.sql
prepare_mysql
echo "creating TiDB db..."
prepare_tidb
echo "creating Vault..."
prepare_vault
