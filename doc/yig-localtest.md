1. 本地编译yig YIG编译方法.md  

2. 本地安装hbase  
   默认启动hbase自带单机zookeeper可以使用  
   
3. 修改dnsmasq  
   修改dnsmsq.conf  
   address=/.s3.test.com/127.0.0.1  
   
4. 修改一些配置文件并且测试  
   cd test  
   修改config.py，指向本地endpointer,例如s3.test.com  
   python sanity.py  
   python object.py  
   python bucket.py  
   python post-policy.py  
   python multipart.py  
   python versioning.py  