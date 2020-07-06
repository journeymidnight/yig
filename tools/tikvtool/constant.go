package main

import "github.com/journeymidnight/yig/meta/client/tikvclient"

const (
	TableBuckets    = "buckets"
	TableUsers      = "users"
	TableObjects    = "objects"
	TableMultiParts = "multiparts"
	TableParts      = "parts"
	TableClusters   = "clusters"
	TableGc         = "gc"
	TableRestore    = "restore" // freezer
	TableHotObjects = "hotobjects"
	TableQos        = "qos"
)

type Table struct {
	Prefix string
}

var TableMap = map[string]Table{
	TableBuckets: {
		Prefix: tikvclient.TableBucketPrefix,
	},
	TableUsers: {
		Prefix: tikvclient.TableUserBucketPrefix,
	},
	TableObjects: {
		Prefix: "",
	},
	TableMultiParts: {
		Prefix: tikvclient.TableMultipartPrefix,
	},
	TableParts: {
		Prefix: tikvclient.TableObjectPartPrefix,
	},
	TableClusters: {
		Prefix: tikvclient.TableClusterPrefix,
	},
	TableGc: {
		Prefix: tikvclient.TableGcPrefix,
	},
	TableRestore: {
		Prefix: tikvclient.TableFreezerPrefix,
	},
	TableHotObjects: {
		Prefix: tikvclient.TableHotObjectPrefix,
	},
	TableQos: {
		Prefix: tikvclient.TableQoSPrefix,
	},
}
