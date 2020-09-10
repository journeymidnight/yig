package main

import "errors"

const (
	TableBuckets           = "buckets"
	TableUsers             = "users"
	TableObjects           = "objects"
	TableObjectPart        = "objectpart"
	TableMultiParts        = "multiparts"
	TableParts             = "multipartpart"
	TableClusters          = "cluster"
	TableGc                = "gc"
	TableRestore           = "restoreobjects" // freezer
	TableRestoreObjectPart = "restoreobjectpart"
	TableHotObjects        = "hotobjects"
	TableQos               = "qos"
	TableLifeCycle         = "lifecycle"

	DefaultDatabase      = "yig"
	DefaultCaddyDatabase = "caddy"
)

var (
	ErrInvalidLine = errors.New("invalid line")
	ErrNoSuchUser  = errors.New("no such user")
)
