GOPATH = $(PWD)/build
export GOPATH
GOBIN = $(PWD)/build/bin
export GOBIN
URL = github.com/journeymidnight
REPO = yig
URLPATH = $(PWD)/build/src/$(URL)

build:
	@[ -d $(URLPATH) ] || mkdir -p $(URLPATH)
	@[ -d $(GOBIN) ] || mkdir -p $(GOBIN)
	@ln -nsf $(PWD) $(URLPATH)/$(REPO)
	go build $(URL)/$(REPO)
	go build $(URLPATH)/$(REPO)/tools/admin.go
	go build $(URLPATH)/$(REPO)/tools/delete.go
	go build $(URLPATH)/$(REPO)/tools/getrediskeys.go
	go build $(URLPATH)/$(REPO)/tools/lc.go
	cp -f yig $(PWD)/build/bin/
	cp -f admin $(PWD)/build/bin/
	cp -f delete $(PWD)/build/bin/
	cp -f getrediskeys $(PWD)/build/bin/
	cp -f lc $(PWD)/build/bin/
pkg:
	sudo docker run --rm -v ${PWD}:/work -w /work yig bash -c 'bash package/rpmbuild.sh'
image:
	sudo docker build -t  yig . -f integrate/yig.docker

run: image
	cd integrate && sudo bash runyig.sh && sudo bash rundelete.sh

env:
	cd integrate && sudo docker-compose stop && sudo docker-compose rm --force && sudo rm -rf cephconf && sudo docker-compose up -d && sleep 20 && sudo bash prepare_env.sh
	

integrate: env run 
	sudo python test/sanity.py
