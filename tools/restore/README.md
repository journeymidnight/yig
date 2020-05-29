# yig-restore
该项目提供了使用yig时的归档存储解冻的方式

## 概要
<p>归档存储解冻功能子模块包括对象的解冻操作（RestoreObject）、解冻对象的零时文件过期销毁（EliminateObject）、解冻对象解冻失败的再解冻（ContinueRestoreNotFinished）。</p>
<p>此方法解冻为跨集群拷贝，主要使用底层为分布式存储Ceph。</p>
<p>解冻对象操作，依托类linux定时任务crontab的定时巡查。</p>

## 数据表
包含两张数据表：restoreobjects、restoreobjectpart，分别用于存储相应的解冻零时对象的元数据及其分片元数据。
