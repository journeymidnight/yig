GOPATH = $(PWD)/build
export GOPATH
GOBIN = $(PWD)/build/bin
export GOBIN
URL = github.com/journeymidnight
REPO = yig
URLPATH = $(PWD)/build/src/$(URL)

all:
	@[ -d $(URLPATH) ] || mkdir -p $(URLPATH)
	@ln -nsf $(PWD) $(URLPATH)/$(REPO)
	go install $(URL)/$(REPO)
	go build $(URLPATH)/$(REPO)/tools/admin.go
	go build $(URLPATH)/$(REPO)/tools/delete.go
	go build $(URLPATH)/$(REPO)/tools/getrediskeys.go
	go build $(URLPATH)/$(REPO)/tools/lc.go
	cp -f admin $(PWD)/build/bin
	cp -f delete $(PWD)/build/bin
	cp -f getrediskeys $(PWD)/build/bin
	cp -f lc $(PWD)/build/bin
