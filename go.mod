module github.com/journeymidnight/yig

go 1.12

replace (
	cloud.google.com/go => github.com/googleapis/google-cloud-go v0.37.4

	github.com/Sirupsen/logrus => github.com/sirupsen/logrus v1.4.1

	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20190404164418-38d8ce5564a5
	golang.org/x/exp => github.com/golang/exp v0.0.0-20190409044807-56b785ea58b2
	golang.org/x/image => github.com/golang/image v0.0.0-20190321063152-3fc05d484e9f
	golang.org/x/lint => github.com/golang/lint v0.0.0-20190313153728-d0100b6bd8b3
	golang.org/x/mobile => github.com/golang/mobile v0.0.0-20190327163128-167ebed0ec6d
	golang.org/x/net => github.com/golang/net v0.0.0-20190404232315-eb5bcb51f2a3
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20190402181905-9f3314589c9a
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys => github.com/golang/sys v0.0.0-20190405154228-4b34438f7a67
	golang.org/x/text => github.com/golang/text v0.3.0
	golang.org/x/time => github.com/golang/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools => github.com/golang/tools v0.0.0-20190408220357-e5b8258f4918
	google.golang.org/api => github.com/googleapis/google-api-go-client v0.3.2
	google.golang.org/appengine => github.com/golang/appengine v1.5.0

	google.golang.org/genproto => github.com/google/go-genproto v0.0.0-20190404172233-64821d5d2107
	google.golang.org/grpc => github.com/grpc/grpc-go v1.19.1
)

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/Sirupsen/logrus v0.0.0-20170822132746-89742aefa4b2
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973
	github.com/cannium/gohbase v0.0.0-20170302080057-636e2cfdbc29
	github.com/cep21/circuit v0.0.0-20181030180945-e893c027dc21
	github.com/dgrijalva/jwt-go v0.0.0-20180921172315-3af4c746e1c2
	github.com/dustin/go-humanize v1.0.0
	github.com/go-redis/redis v6.15.2+incompatible
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.2.0 // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.1 // indirect
	github.com/gomodule/redigo v1.7.0
	github.com/gorilla/mux v1.6.2
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.5.3 // indirect
	github.com/hashicorp/go-rootcerts v1.0.0 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/vault v0.0.0-20181205182516-af78f40cf86f
	github.com/journeymidnight/aws-sdk-go v1.17.5 // indirect
	github.com/journeymidnight/radoshttpd v0.0.0-20180423094851-048fda5702ec
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/minio/highwayhash v1.0.0
	github.com/mitchellh/mapstructure v1.1.2 // indirect
	github.com/pierrec/lz4 v2.0.5+incompatible // indirect
	github.com/prometheus/client_golang v0.9.3-0.20190127221311-3c4408c8b829
	github.com/prometheus/client_model v0.0.0-20190115171406-56726106282f
	github.com/prometheus/common v0.2.0
	github.com/prometheus/procfs v0.0.0-20190117184657-bf6a532e95b1
	github.com/ryanuber/go-glob v1.0.0 // indirect
	github.com/samuel/go-zookeeper v0.0.0-20180130194729-c4fab1ac1bec
	github.com/ugorji/go v1.1.4
	github.com/xxtea/xxtea-go v0.0.0-20170828040851-35c4b17eecf6
	golang.org/x/net v0.0.0-20190311183353-d8887717615a // indirect
	golang.org/x/sync v0.0.0-20190227155943-e225da77a7e6 // indirect
	golang.org/x/text v0.3.1-0.20180807135948-17ff2d5776d2 // indirect
	golang.org/x/time v0.0.0-00010101000000-000000000000 // indirect
	gopkg.in/check.v1 v0.0.0-20161208181325-20d25e280405
)
