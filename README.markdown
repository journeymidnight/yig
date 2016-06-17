build requries:

ceph-devel
go1.6

go get -u github.com/codegangsta/martini


start server:
0. put project to $GOPATH/src/git.letv.cn/yig
1. mkdir -p /var/log/yig/
2. ./yig
3. curl http://127.0.0.1:3000/info/rbd/test

