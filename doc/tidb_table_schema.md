
## buckets
|   Column   	|   Type   	| NotNull 	| Remark 	|
|:----------:	|:--------:	|:-------:	|:------:	|
| bucketname 	|  string  	|    T    	|        	|
|     acl    	|  string  	|    F    	|   JSON  	|
|    cors    	|  string  	|    F    	|   JSON  	|
|     lc     	|  string  	|    F    	|   JSON   	|
|     uid    	|  string  	|    T    	|        	|
|   policy   	|  string  	|    F    	|   JSON   	|
| createtime 	| datetime 	|    F    	|        	|
|   usages   	|  uint64  	|    T    	|        	|
| versioning 	|  string  	|    F    	|        	|

## cluster
UNIQUE KEY `rowkey` (`fsid`,`pool`)

| Column 	|  Type  	| NotNull 	| Remark 	|
|:------:	|:------:	|:-------:	|:------:	|
|  fsid  	| string 	|    F    	|        	|
|  pool  	| string 	|    F    	|        	|
| weight 	|   int  	|    F    	|        	|

## users
|   Column   	|  Type  	| NotNull 	| Remark 	|
|:----------:	|:------:	|:-------:	|:------:	|
|   userid   	| string 	|    T    	|        	|
| bucketname 	| string 	|    T    	|        	|

## gc
UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)

|   Column   	|  Type  	| NotNull 	| Remark 	|
|:----------:	|:------:	|:-------:	|:------:	|
| bucketname 	| string 	|    F    	|        	|
| objectname 	| string 	|    F    	|        	|
|   version  	| uint64 	|    F    	|        	|
|  location  	| string 	|    F    	|        	|
|    pool    	| string 	|    F    	|        	|
|  objectid  	| string 	|    F    	|        	|
|   status   	| string 	|    F    	|        	|
|    mtime   	| datetime 	|    F    	|        	|
|    part    	|  bool  	|    F    	|        	|
| triedtimes 	|   int  	|    F    	|        	|

## gcpart
UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)

|        Column        	|  Type  	| NotNull 	| Remark 	|
|:--------------------:	|:------:	|:-------:	|:------:	|
|      partnumber      	|   int  	|    F    	|        	|
|         size         	|  int64 	|    F    	|        	|
|       objectid       	| string 	|    F    	|        	|
|        offset        	|  int64 	|    F    	|        	|
|         etag         	| string 	|    F    	|        	|
|     lastmodified     	| datetime 	|    F    	|        	|
| initializationvector 	|  blob  	|    F    	|        	|
|      bucketname      	| string 	|    F    	|        	|
|      objectname      	| string 	|    F    	|        	|
|        version       	| uint64 	|    F    	|        	|

## multiparts
UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`uploadtime`)

|    Column   	|  Type  	| NotNull 	| Remark 	|
|:-----------:	|:------:	|:-------:	|:------:	|
|  bucketname 	| string 	|    F    	|        	|
|  objectname 	| string 	|    F    	|        	|
|  uploadtime 	| uint64 	|    F    	|        	|
| initiatorid 	| string 	|    F    	|        	|
|   ownerid   	| string 	|    F    	|        	|
| contenttype 	| string 	|    F    	|        	|
|   location  	| string 	|    F    	|        	|
|     pool    	| string 	|    F    	|        	|
|     acl     	| string 	|    F    	|   JSON   	|
|  sserequest 	| string 	|    F    	|   JSON   	|
|  encryption 	|  blob  	|    F    	|        	|
|    attrs    	| string 	|    F    	|   JSON   	|

## multipartpart
UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`uploadtime`)

|        Column        	|  Type  	| NotNull 	| Remark 	|
|:--------------------:	|:------:	|:-------:	|:------:	|
|      partnumber      	|   int  	|    F    	|        	|
|         size         	|  int64 	|    F    	|        	|
|       objectid       	| string 	|    F    	|        	|
|        offset        	|  int64 	|    F    	|        	|
|         etag         	| string 	|    F    	|        	|
|     lastmodified     	| datetime 	|    F    	|        	|
| initializationvector 	|  blob  	|    F    	|        	|
|      bucketname      	| string 	|    F    	|        	|
|      objectname      	| string 	|    F    	|        	|
|      uploadtime      	| uint64 	|    F    	|        	|

## objects
UNIQUE KEY `rowkey` (`bucketname`,`name`,`version`)

|        Column        	|   Type   	| NotNull 	| Remark 	|
|:--------------------:	|:--------:	|:-------:	|:------:	|
|      bucketname      	|  string  	|    F    	|        	|
|         name         	|  string  	|    F    	|        	|
|        version       	|  uint64  	|    F    	|        	|
|       location       	|  string  	|    F    	|        	|
|         pool         	|  string  	|    F    	|        	|
|        ownerid       	|  string  	|    F    	|        	|
|         size         	|   int64  	|    F    	|        	|
|       objectid       	|  string  	|    F    	|        	|
|   lastmodifiedtime   	| datetime 	|    F    	|        	|
|         etag         	|  string  	|    F    	|        	|
|      contenttype     	|  string  	|    F    	|       	|
|   customattributes   	|  string  	|    F    	|   JSON   	|
|          acl         	|  string  	|    F    	|   JSON   	|
|      nullversion     	|   bool   	|    F    	|        	|
|     deletemarker     	|   bool   	|    F    	|        	|
|        ssetype       	|  string  	|    F    	|        	|
|     encryptionkey    	|   blob   	|    F    	|        	|
| initializationvector 	|   blob   	|    F    	|        	|

## objectpart
UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)

|        Column        	|  Type  	| NotNull 	| Remark 	|
|:--------------------:	|:------:	|:-------:	|:------:	|
|      partnumber      	|   int  	|    F    	|        	|
|         size         	|  int64 	|    F    	|        	|
|       objectid       	| string 	|    F    	|        	|
|        offset        	|  int64 	|    F    	|        	|
|         etag         	| string 	|    F    	|        	|
|     lastmodified     	| datetime 	|    F    	|        	|
| initializationvector 	|  blob  	|    F    	|        	|
|      bucketname      	| string 	|    F    	|        	|
|      objectname      	| string 	|    F    	|        	|
|        version       	| string 	|    F    	|        	|

## objmap
UNIQUE KEY `objmap` (`bucketname`,`objectname`)

|   Column   	|  Type  	| NotNull 	| Remark 	|
|:----------:	|:------:	|:-------:	|:------:	|
| bucketname 	| string 	|    F    	|        	|
| objectname 	| string 	|    F    	|        	|
| nullvernum 	|  int64 	|    F    	|        	|