# YIG

LeStorage gateway implementation that fully compatible with Amazon S3, young, but not naive.

See http://wiki.letv.cn/pages/viewpage.action?pageId=55651555 for development documentation.

### Build

Require:

- ceph-devel
- go(>=1.7)

Steps:

```shell
mkdir -p $GOPATH/src/git.letv.cn/yig
cd $GOPATH/src/git.letv.cn/yig
git clone git@git.letv.cn:yig/yig.git 
cd $YIG_DIR
go get ./...
go build
```


Start server:
```shell
cd $YIG_DIR
sudo ./yig
```

