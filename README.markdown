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
mkdir -p $GOPATH/src/legitlab.letv.cn/yig
cd $GOPATH/src/legitlab.letv.cn/yig
git clone git@legitlab.letv.cn:yig/yig.git 
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

