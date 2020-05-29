URL = github.com/journeymidnight
REPO = yig-restore
VERSION?=v0.0.1
RELEASE?=rc01
PWD=$(shell pwd)
ARCH=linux_amd64
BIN_NAME=$(REPO)_$(VERSION)-$(RELEASE)_$(ARCH)
export GO111MODULE=on
export GOPROXY=https://goproxy.cn

.PHONY: build

build:
	go build $(URL)/$(REPO)
	bash plugins/build_plugins.sh

pkg:
	mkdir -p $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	go build $(URL)/$(REPO)
	bash plugins/build_plugins.sh
	@cp ./$(REPO)*  $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	@cp ./package/*  $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	@cp ./plugins/*.so  $(PWD)/rpmbuild/SOURCES/$(BIN_NAME)
	cd $(PWD)/rpmbuild/SOURCES && tar cvfz $(BIN_NAME).tar.gz $(BIN_NAME)
	rpmbuild --define '_rpmfilename $(BIN_NAME).rpm' --define '_topdir $(PWD)/rpmbuild' --define 'version $(VERSION)' --define 'release $(RELEASE)' -ba --clean  $(PWD)/package/$(REPO).spec
	@cp ./rpmbuild/RPMS/$(BIN_NAME).rpm ./$(BIN_NAME).rpm
	@rm -rf ./rpmbuild
	@rm -rf ./rpm-build