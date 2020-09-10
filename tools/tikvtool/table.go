package main

import "github.com/journeymidnight/yig/meta/client/tikvclient"

type TableConverter interface {
	Parse(dml []byte, ref interface{}) (err error)
	Convert(ref interface{}) (err error)
}

type Table struct {
	Name        string
	Prefix      string
	ExistInTiKV bool
	Converter   TableConverter
}

var (
	TblBuckets = Table{
		Name:        TableBuckets,
		Prefix:      tikvclient.TableBucketPrefix,
		ExistInTiKV: true,
		Converter:   BucketConverter{},
	}
	TblObjects = Table{
		Name:        TableObjects,
		Prefix:      "",
		ExistInTiKV: true,
		Converter:   ObjectConverter{},
	}
	TblLifeCycle = Table{
		Name:        TableLifeCycle,
		Prefix:      tikvclient.TableLifeCyclePrefix,
		ExistInTiKV: true,
		Converter:   LifeCycleConverter{},
	}
	TblMultiParts = Table{
		Name:        TableMultiParts,
		Prefix:      tikvclient.TableMultipartPrefix,
		ExistInTiKV: true,
		Converter:   MultipartsConverter{},
	}
	TblObjectPart = Table{
		Name:        TableObjectPart,
		Prefix:      "",
		ExistInTiKV: false,
		Converter:   ObjectPartConverter{},
	}
	TblUsers = Table{
		Name:        TableUsers,
		Prefix:      tikvclient.TableUserBucketPrefix,
		ExistInTiKV: true,
		Converter:   UserConverter{},
	}
	TblParts = Table{
		Name:        TableParts,
		Prefix:      tikvclient.TableObjectPartPrefix,
		ExistInTiKV: true,
		Converter:   PartsConverter{},
	}
	TblClusters = Table{
		Name:        TableClusters,
		Prefix:      tikvclient.TableClusterPrefix,
		ExistInTiKV: true,
		Converter:   ClusterConverter{},
	}
	TblGc = Table{
		Name:        TableGc,
		Prefix:      tikvclient.TableGcPrefix,
		ExistInTiKV: true,
		Converter:   nil,
	}
	TblRestore = Table{
		Name:        TableRestore,
		Prefix:      tikvclient.TableFreezerPrefix,
		ExistInTiKV: true,
		Converter:   RestoreConverter{},
	}
	TblRestoreObjectPart = Table{
		Name:        TableRestoreObjectPart,
		Prefix:      "",
		ExistInTiKV: false,
		Converter:   RestoreObjectPartConverter{},
	}
	TblHotObjects = Table{
		Name:        TableHotObjects,
		Prefix:      tikvclient.TableHotObjectPrefix,
		ExistInTiKV: true,
		Converter:   HotObjectsConverter{},
	}
	TblQos = Table{
		Name:        TableQos,
		Prefix:      tikvclient.TableQoSPrefix,
		ExistInTiKV: true,
		Converter:   QosConverter{},
	}
)

var TableMap = map[string]Table{
	TableBuckets:           TblBuckets,
	TableObjects:           TblObjects,
	TableLifeCycle:         TblLifeCycle,
	TableMultiParts:        TblMultiParts,
	TableObjectPart:        TblObjectPart,
	TableUsers:             TblUsers,
	TableParts:             TblParts,
	TableClusters:          TblClusters,
	TableGc:                TblGc,
	TableRestore:           TblRestore,
	TableRestoreObjectPart: TblRestoreObjectPart,
	TableHotObjects:        TblHotObjects,
	TableQos:               TblQos,
}
