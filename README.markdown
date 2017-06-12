# YIG

LeStorage gateway implementation that fully compatible with Amazon S3, young, but not naive.

See http://wiki.letv.cn/pages/viewpage.action?pageId=55651555 for development documentation.

### Build


How to build?

http://wiki.letv.cn/pages/viewpage.action?pageId=64550662

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

Start server:
```shell
cd $YIG_DIR
sudo ./yig
```

OR 

```
systemctl start yig
```

