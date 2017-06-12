#!/bin/bash
filepath=$(cd "$(dirname "$0")"; pwd)
cd $filepath

repo='10.140.75.88'
remotedir="/letv/yum-repo/ceph-jewel/el7/update"
sshpasspath=`which sshpass`
if [[ $? -ne  0 ]] ;then
        echo "try to install sshpass first"
        yum install sshpass -y
        sshpasspath=`which sshpass`
fi
$sshpasspath -p TVLEhp800g.com scp -o StrictHostKeyChecking=no $filepath/*.rpm root@$repo:$remotedir/x86_64
$sshpasspath -p TVLEhp800g.com ssh root@$repo createrepo --update $remotedir
$sshpasspath -p TVLEhp800g.com ssh root@$repo /letv/yum-repo/sync.ceph-jewel
