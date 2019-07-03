.PHONY: build
GOPATH = $(PWD)/build
export GOPATH
GOBIN = $(PWD)/build/bin
export GOBIN
URL = github.com/journeymidnight
REPO = yig
URLPATH = $(PWD)/build/src/$(URL)

build:
	cd integrate && bash buildyig.sh

build_internal:
	@[ -d $(URLPATH) ] || mkdir -p $(URLPATH)
	@[ -d $(GOBIN) ] || mkdir -p $(GOBIN)
	@ln -nsf $(PWD) $(URLPATH)/$(REPO)
	go build $(URL)/$(REPO)
	go build -buildmode=plugin -o iam.so $(URLPATH)/$(REPO)/iam_plugin.go
	go build $(URLPATH)/$(REPO)/tools/admin.go
	go build $(URLPATH)/$(REPO)/tools/delete.go
	go build $(URLPATH)/$(REPO)/tools/getrediskeys.go
	go build $(URLPATH)/$(REPO)/tools/lc.go
	cp -f yig $(PWD)/build/bin/
	cp -f iam.so $(PWD)/integrate/yigconf/plugins/
	cp -f admin $(PWD)/build/bin/
	cp -f delete $(PWD)/build/bin/
	cp -f getrediskeys $(PWD)/build/bin/
	cp -f lc $(PWD)/build/bin/
pkg:
	docker run --rm -v $(PWD):/work -w /work journeymidnight/yig bash -c 'bash package/rpmbuild.sh'
image:
	docker build -t  journeymidnight/yig . -f integrate/yig.docker

run: 
	cd integrate && bash runyig.sh

rundelete:
	cd integrate && sudo bash rundelete.sh

env:
	cd integrate && docker-compose stop && docker-compose rm --force && sudo rm -rf cephconf && docker-compose up -d && sleep 20 && bash prepare_env.sh
	

integrate: env build run 

clean:
	cd integrate && docker-compose stop && docker-compose rm --force &&rm -rf cephconf
	rm -rf build
