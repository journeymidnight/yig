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
	go build $(URLPATH)/$(REPO)/tools/admin.go
	go build $(URLPATH)/$(REPO)/tools/delete.go
	go build $(URLPATH)/$(REPO)/tools/getrediskeys.go
	go build $(URLPATH)/$(REPO)/tools/lc.go
	cd $(URLPATH)/$(REPO)/test/ci && export GO111MODULE=on && go test -c -o yigci
	cp -f yig $(PWD)/build/bin/
	cp -f admin $(PWD)/build/bin/
	cp -f delete $(PWD)/build/bin/
	cp -f getrediskeys $(PWD)/build/bin/
	cp -f lc $(PWD)/build/bin/

pkg:
	sudo docker run --rm -v $(PWD):/work -w /work journeymidnight/yig bash -c 'bash package/rpmbuild.sh'
image:
	sudo docker build -t  journeymidnight/yig . -f integrate/yig.docker

run: 
	cd integrate && sudo bash runyig.sh

rundelete:
	cd integrate && sudo bash rundelete.sh

env:
	cd integrate && sudo docker-compose stop && sudo docker-compose rm --force && sudo rm -rf cephconf && sudo docker-compose up -d && sleep 20 && sudo bash prepare_env.sh
	

integrate: env build run 

clean:
	cd integrate && docker-compose stop && docker-compose rm --force &&rm -rf cephconf
	rm -rf build
