package client

import (
	"github.com/journeymidnight/yig/api/datatype"
	. "github.com/journeymidnight/yig/meta/types"
)

func ModifyMetaToObjectResult(objMeta Object) datatype.Object {
	var o datatype.Object
	o.Key = objMeta.Name
	o.Owner = datatype.Owner{ID: objMeta.OwnerId}
	o.ETag = objMeta.Etag
	o.LastModified = objMeta.LastModifiedTime.Format(CREATE_TIME_LAYOUT)
	o.Size = objMeta.Size
	o.StorageClass = objMeta.StorageClass.ToString()
	return o
}

func ModifyMetaToVersionedObjectResult(objMeta Object) datatype.VersionedObject {
	var o datatype.VersionedObject
	o.Key = objMeta.Name
	o.Owner = datatype.Owner{ID: objMeta.OwnerId}
	o.ETag = objMeta.Etag
	o.LastModified = objMeta.LastModifiedTime.Format(CREATE_TIME_LAYOUT)
	o.Size = objMeta.Size
	o.StorageClass = objMeta.StorageClass.ToString()
	if objMeta.VersionId == NullVersion {
		objMeta.VersionId = "null"
	}
	o.VersionId = objMeta.VersionId
	o.DeleteMarker = objMeta.DeleteMarker
	return o
}
