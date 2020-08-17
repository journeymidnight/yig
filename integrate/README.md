# Yig关于CI的使用说明

### 概述
以下主要是描述Drone CI&CD使用方式，将分为几个部分说明：触发条件、工作流、CD部署。该系统主要是借助Docker将各项目或是各分支之间的提交进行隔离，以此达到自动并发构建，并发脚本测试的目的。其中构建代码所使用的docker容器是基于CentOS的docker镜像进行再构建而成。
由于yig的特殊性，需要多种依赖组件，故测试环境借助于物理环境机器进行，所以如果您需要特殊依赖，需要准备相应的依赖环境，并修改相应的配置文件，下文将会详细说明。
若其它项目无其它依赖，直接使用容器即可完成构建，详情请参考本地仓库Yig-front-caddy的Drone CI&CD自动化构建配置。

### 触发条件
* 本地代码仓库进行代码提交时：每次您的代码push操作，CI会自动检测到该触发条件，并且自动为进行代码构建和测试。
* 向目标分支提交MR请求时：当向指定分支提交代码时，我们还将触发打包操作，详情可以根据需求配置.drone.yml文件。此时依旧会进行相应的构建和检查，以保证解决冲突后代码不会出现较大问题。
* 提交版本TAG时：提交版本后，将会根据提交的分支对应的最新的commit号自动生成相应的版本包，并传输到目标地址。这里我们配置的是北京的线上存储桶内，相应的配置方法也会在下文详细说明。

### 工作流
工作流指的是具体的构建与操作流程，这部分我将详细说明如何配置相应的触发器。

##### 仓库配置
* 首先我们来了解下我们所制定的工作流，请先看下图：
![arch](http://oss-ref.oss-cn-north-1.unicloudsrv.com/CI%26CD_pipline.png)
上图中详细描绘出了我们所规划的分支，并且说明何时进行分支创建、分支合并、分支删除。旁边则说明了相应的版本号说明、分支名称说明以及打包规则说明。

* 接下来我们来看看如何构成这样一个工作流：
首先需要使用仓库master以上权限的帐号到Drone的配置界面配置相应的设置才可以使用相应的仓库。我们仓库的内网地址为：10.0.42.88
直接访问CI地址，它将要求获取我们的GitLab仓库信息，授权获取。
![arch](http://oss-ref.oss-cn-north-1.unicloudsrv.com/authorize.png)

* 获取授权后将跳转返回CI的主界面，找到右上角的SYNC刷新仓库，就可以看到需要配置的仓库了。（注意：这里仓库的名称如果超过两级目录将无法显示，是目前Drone暂未修复的一个问题）点击您需要配置的仓库名称，这里我们Yig的仓库我已经配置完成了，下图我直接展示配置完成的信息。
![arch](http://oss-ref.oss-cn-north-1.unicloudsrv.com/setting.png)

* 这里有几个配置需要说明一下，第一个是Project settings，这个配置项决定你是否信任该仓库进行直接构建，或是开启容器特权模式构建。选择Protected模式，每次构建时，需要进入CI界面授权信任，才能开始构建；Trusted模式则是完全信任仓库，可以使用特权模式开启容器，获取容器中最高权限。
第一部分就推荐使用我的配置，其余的参考Drone官方文档。
第二部分，Secrets。主要是为了避免直接在代码中出现相应的远程密钥或是服务器信息，在此配置了相应的字段后，可以在.drone.yml文件中进行读取，上图中，我配置了ftp的用户名以及密码、S3连接的aksk，具体根据仓库实际情况需要进行配置。
第三部分，Cron Jobs。这部分主要是实现一些定期任务的部分，可以接受Shell脚本命令的形式，具体请参考Drone官网。

##### 工作流配置文件说明
工作流实现主要是根据配置中配置的.drone.yml文件实现，当然可根据项目具体情况进行修改。下面我将Yig项目的工作流配置文件贴到下面进行讲解。
```
---
kind: pipeline
type: docker
name: yig

clone:
  git:
    image: plugins/git

platform:
  os: linux
  arch: amd64

workspace:
  path: /drone/src/yig

steps:
- name: build & test
  image: centos-yig:v1.0.1
  commands:
  - export GOPROXY=https://goproxy.cn
  - cp -f integrate/resolv.conf /etc/resolv.conf
  - make build_internal
  - sqlName=yig_drone${DRONE_BUILD_NUMBER}
  - mysql -u root -P 4000 -h 10.0.42.4 -e "create database $sqlName character set utf8;"
  - mysql -u root -P 4000 -h 10.0.42.4 -e "use $sqlName;source integrate/yig.sql;"
  - cp -f integrate/test-env/yig.toml /etc/yig/yig.toml
  - cp -f plugins/*.so /etc/yig/plugins/
  - sed -i 's/yig_drone/yig_drone${DRONE_BUILD_NUMBER}/' /etc/yig/yig.toml
  - sed -i 's/cluster/single/' /etc/yig/yig.toml
  - /usr/sbin/dnsmasq -k &
  - nohup ./yig >/dev/null 2>&1 &
  - sleep 10
  - ./lc &
  - sleep 10
  - ./migrate &
  - sleep 10
  - ping -c 1 s3.test.com
  - ps aux|grep yig
  - ps aux|grep lc
  - ps aux|grep migrate
  - python test/sanity.py
  - pushd test/go
  - go test -v
  - popd
  - mysql -u root -P 4000 -h 10.0.42.4 -e "drop database $sqlName;"
  when:
    event:
      exclude:
      - tag


- name: clean up database
  image: centos-yig:v1.0.1
  commands:
  - sqlName=yig_drone${DRONE_BUILD_NUMBER}
  - mysql -u root -P 4000 -h 10.0.42.4 -e "drop database $sqlName;"
  when:
    event:
    - failure

- name: make package with test
  image: centos-yig:v1.0.1
  environment:
    VER_DRONE: ${DRONE_SOURCE_BRANCH}
    REL_DRONE: ALPHA
  commands:
  - export GOPROXY=https://goproxy.cn
  - make pkg_internal
  when:
    branch:
    - develop
    event:
    - pull_request
  depends_on:
  - build & test

- name: update package to ftp
  image: cschlosser/drone-ftps
  environment:
    FTP_USERNAME:
      from_secret: ftp_user_name
    FTP_PASSWORD:
      from_secret: ftp_user_pass
    PLUGIN_HOSTNAME: 10.0.47.182
    PLUGIN_VERIFY: false
    PLUGIN_SECURE: false
    PLUGIN_DEST_DIR: /pub/Untested_Packages/yig
    PLUGIN_SRC_DIR: /packages/
  when:
    branch:
    - develop
    event:
    - pull_request
  depends_on:
  - make package with test

- name: deploy the test package to the target environment
  image: centos-yig:v1.0.1
  commands:
  - cd integrate
  - mysql -u root -h 10.0.42.4 -P 4000 -e "drop database yig_drone;"
  - mysql -u root -h 10.0.42.4 -P 4000 -e "create database yig_drone character set utf8;"
  - mysql -u root -h 10.0.42.4 -P 4000 -e "use yig_drone;source yig.sql;"
  - curDate=yig_$(date "+%Y%m%d-%H%M%S")
  - ssh 10.0.42.5 "mkdir /test_packages/$curDate;exit"
  - scp test-env/yig.toml 10.0.42.5:/test_packages/$curDate
  - scp ../package/*.x86_64.rpm 10.0.42.5:/test_packages/$curDate
  - ssh 10.0.42.5 "cd /test_packages;sh update_yig_package.sh $curDate"
  when:
    branch:
    - develop
    event:
    - pull_request
  depends_on:
  - make package with test


- name: make package with published
  image: centos-yig:v1.0.1
  environment:
    VER_DRONE: ${DRONE_TAG=latest}
    REL_DRONE: PUB
  commands:
    - export GOPROXY=https://goproxy.cn
    - make pkg_internal
  when:
    event:
    - tag

- name: update package to s3
  image: plugins/s3
  settings:
    bucket: published-packages
    access_key:
      from_secret: s3_access_key
    secret_key:
      from_secret: s3_secret_key
    acl: public-read
    source: /drone/src/yig/packages/*
    target: /yig/
    strip_prefix: /drone/src/yig/packages/
    path_style: true
    endpoint: http://oss-cn-north-2.unicloudsrv.com
  when:
    event: tag
  depends_on:
  - make package with published

trigger:
  event:
  - push
  - tag
  - pull_request

```
以上是一个完整的工作流配置文件，它包括了三个部分：工作流信息、工作步骤、触发器
* 工作流信息：
```
kind: pipeline
type: docker
name: yig

clone:
  git:
    image: plugins/git

platform:
  os: linux
  arch: amd64

workspace:
  path: /drone/src/yig
```
这部分包含了对于构建项目的各种说明，它包括：类型、构建项目名称、代码拉取方式、构建环境、构建空间。其中我就特别说明以下代码拉取方式和构建空间。
##### 代码拉取方式
这里代码拉取使用的是默认模式，及拉取构建分支并fetch目标分支代码的方式进行构建，如果产生冲突是无法自动解决的，如果需要使用其它的构建方式，需要手动配置在下面的工作步骤中手动配置，并去除这块代码块。但是不推荐这么做，因为它将及其的麻烦。
##### 构建空间
构建空间指的是在容器中创建一个相应的目录空间，以存放你所提交的代码文件，需要注意的是，代码空间的代码是公共的，哪怕你在下面的工作步骤中使用多个容器进行协作，都可以使用其中的文件，所以需要避免冲突的产生。

* 工作步骤
以下是工作步骤配置的单个容器的截取：
```
- name: build & test
  image: centos-yig:v1.0.1
  commands:
  - export GOPROXY=https://goproxy.cn
  - cp -f integrate/resolv.conf /etc/resolv.conf
  - make build_internal
  - sqlName=yig_drone${DRONE_BUILD_NUMBER}
  - mysql -u root -P 4000 -h 10.0.42.4 -e "create database $sqlName character set utf8;"
  - mysql -u root -P 4000 -h 10.0.42.4 -e "use $sqlName;source integrate/yig.sql;"
  - cp -f integrate/test-env/yig.toml /etc/yig/yig.toml
  - cp -f plugins/*.so /etc/yig/plugins/
  - sed -i 's/yig_drone/yig_drone${DRONE_BUILD_NUMBER}/' /etc/yig/yig.toml
  - sed -i 's/cluster/single/' /etc/yig/yig.toml
  - /usr/sbin/dnsmasq -k &
  - nohup ./yig >/dev/null 2>&1 &
  - sleep 10
  - ./lc &
  - sleep 10
  - ./migrate &
  - sleep 10
  - ping -c 1 s3.test.com
  - ps aux|grep yig
  - ps aux|grep lc
  - ps aux|grep migrate
  - python test/sanity.py
  - pushd test/go
  - go test -v
  - popd
  - mysql -u root -P 4000 -h 10.0.42.4 -e "drop database $sqlName;"
  when:
    event:
      exclude:
      - tag
```
以上是一个完整的工作子步骤，它包含了：步骤名称、docker镜像名称、操作脚本、触发条件。
##### docker镜像名称
这里需要注意的是，这里的docker镜像必须是CI机器上所存在的镜像，或是能在docker hub进行下载的公共镜像，否则将构建失败。
##### 操作脚本
这里的操作脚本即是你需要对代码进行的操作，如构建和测试等，需要注意的是，你的操作步骤部分可能和直接的物理环境存在差别，需要考虑docker容器的特性，如DNS解析文件/etc/resolv.conf文件不能直接使用sed命令进行修改。
##### 触发条件
即说明当前子步骤在什么情况下会进行触发，以上样例标注的是，出了tag以外都进行触发，详情请参考Drone官方文档，触发条件。

* 触发器
以下是触发器部分，主要是告诉CI工具何时进行相应的工作
```
trigger:
  event:
  - push
  - tag
  - pull_request
```
这部分主要是说明在构成什么仓库事件的时候，能进行以上的工作步骤，这里我配置当提交、MR、tag的时候进行工作，其它配置详见Drone官网。

#### CD部署
CD部署其实也是借助于CI的触发器进行，但是有所不同的是，需要在指定的位置放置相应的脚本，因为目标服务器操作的时候，CI会在工作流结束时清除执行过的操作，所以直接进行部署操作，会导致CI停止之后无法完成部署。
我们的解决办法是提前在服务器上编辑一个将对应的包文件进行安装替换的脚本，并且可以根据需求追加相应的测试用例，进行一体化测试。这里我们需要配置相应的部署环境。yig中主要使用当前目录下的配置文件进行构建、测试以及配置。
如果您需要更改构建配置需要修改当前文件夹下的配置文件。
现在配置的对接环境为42.4集群，所有测试基于42.4集群进行。并且会在42.5机器上进行自动部署及脚本测试。