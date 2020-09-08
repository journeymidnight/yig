package main

import "github.com/journeymidnight/yig/meta/client/tikvclient"

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
)

type Table struct {
	Prefix      string // Prefix in TiKV
	ExistInTiKV bool
	ParseFn     ParseFn
}

var TableMap = map[string]Table{
	TableBuckets: {
		Prefix:      tikvclient.TableBucketPrefix,
		ExistInTiKV: true,
		ParseFn:     parseBucket,
	},
	TableUsers: {
		Prefix:      tikvclient.TableUserBucketPrefix,
		ExistInTiKV: true,
		ParseFn:     parseUsers,
	},
	TableObjects: {
		Prefix:      "",
		ExistInTiKV: true,
		ParseFn:     parseObject,
	},
	TableMultiParts: {
		Prefix:      tikvclient.TableMultipartPrefix,
		ExistInTiKV: true,
		ParseFn:     parseMultiparts,
	},
	TableParts: {
		Prefix:      tikvclient.TableObjectPartPrefix,
		ExistInTiKV: true,
		ParseFn:     parseObjectPart,
	},
	TableClusters: {
		Prefix:      tikvclient.TableClusterPrefix,
		ExistInTiKV: true,
		ParseFn:     parseClusters,
	},
	TableGc: {
		Prefix:      tikvclient.TableGcPrefix,
		ExistInTiKV: true,
	},
	TableRestore: {
		Prefix:      tikvclient.TableFreezerPrefix,
		ExistInTiKV: true,
		ParseFn:     parseRestore,
	},
	TableRestoreObjectPart: {
		Prefix:      "",
		ExistInTiKV: false,
		ParseFn:     parseObjectPart,
	},
	TableHotObjects: {
		Prefix:      tikvclient.TableHotObjectPrefix,
		ExistInTiKV: true,
		ParseFn:     parseObject,
	},
	TableQos: {
		Prefix:      tikvclient.TableQoSPrefix,
		ExistInTiKV: true,
		ParseFn:     parseQos,
	},
	TableLifeCycle: {
		Prefix:      tikvclient.TableLifeCyclePrefix,
		ExistInTiKV: true,
		ParseFn:     parseLifeCycle,
	},
	TableObjectPart: {
		Prefix:      "",
		ExistInTiKV: false,
		ParseFn:     parseObjectPart,
	},
}
