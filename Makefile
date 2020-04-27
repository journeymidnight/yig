.PHONY: build
URL = github.com/journeymidnight
REPO = yig
WORKDIR = /work
BUILDROOT = rpm-build
BUILDDIR = $(WORKDIR)/$(BUILDROOT)/BUILD/$(REPO)
export GO111MODULE=on
export GOPROXY=https://goproxy.cn

build:
	cd integrate && bash buildyig.sh $(BUILDDIR)

build_internal:
	go build $(URL)/$(REPO)
	bash plugins/build_plugins_internal.sh
	go build $(PWD)/tools/admin.go
	go build $(PWD)/tools/delete.go
	go build $(PWD)/tools/getrediskeys.go
	go build $(PWD)/tools/lc.go
	cp -f $(PWD)/plugins/*.so $(PWD)/integrate/yigconf/plugins/

pkg:
	docker run --rm -v $(PWD):$(WORKDIR) -w $(WORKDIR) journeymidnight/yig bash -c 'bash package/rpmbuild.sh $(REPO) $(BUILDROOT)'

image:
	docker build -t  journeymidnight/yig . -f integrate/yig.docker

run: 
	cd integrate && bash runyig.sh $(WORKDIR)

stop:
	cd integrate && bash stopyig.sh

rundelete:
	cd integrate && sudo bash rundelete.sh $(WORKDIR)

runlc:
	cd integrate && sudo bash runlc.sh $(WORKDIR)

env:
	cd integrate && docker-compose stop && docker-compose rm --force && sudo rm -rf cephconf && docker-compose up -d && sleep 20 && bash prepare_env.sh
	
plugin:
	cd plugins && bash build_plugins.sh $(BUILDDIR)

plugin_internal:
	bash plugins/build_plugins_internal.sh


integrate: env build run runlc

clean:
	cd integrate && docker-compose stop && docker-compose rm --force &&rm -rf cephconf
	rm -rf build
