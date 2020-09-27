package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/helper"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/meta/common"

	"github.com/journeymidnight/yig/meta/client/tikvclient"
	"github.com/journeymidnight/yig/meta/types"
)

type BucketConverter struct{}

func parseBucket(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var b types.Bucket
	b.Name = toString(extractConstant(&remain))
	aclStr := extractJson(&remain)
	err = json.Unmarshal(aclStr, &b.ACL)
	if err != nil {
		return
	}

	corStr := extractJson(&remain)
	err = json.Unmarshal(corStr, &b.CORS)
	if err != nil {
		return
	}

	loggingStr := extractJson(&remain)
	err = json.Unmarshal(loggingStr, &b.BucketLogging)
	if err != nil {
		return
	}

	lcStr := extractJson(&remain)
	err = json.Unmarshal(lcStr, &b.Lifecycle)
	if err != nil {
		return
	}

	b.OwnerId = toString(extractConstant(&remain))
	v, ok := reflectMap[b.OwnerId]
	if !ok {
		return ErrNoSuchUser
	}
	b.OwnerId = v
	policyStr := extractJson(&remain)
	b.Policy = policyStr

	websiteStr := extractJson(&remain)
	err = json.Unmarshal(websiteStr, &b.Website)
	if err != nil {
		return
	}

	encryptionStr := extractJson(&remain)
	err = json.Unmarshal(encryptionStr, &b.Encryption)
	if err != nil {
		return
	}

	cTime := toString(extractConstant(&remain))
	b.CreateTime, err = time.Parse(types.TIME_LAYOUT_TIDB, cTime)
	if err != nil {
		return
	}

	// Ignore usage
	extractConstant(&remain)
	b.Versioning = datatype.BucketVersioningType(toString(extractConstant(&remain)))

	*ref.(*types.Bucket) = b
	return
}

func (_ BucketConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseBucket(dml, ref)
}

func (_ BucketConverter) Convert(ref interface{}) (err error) {
	fmt.Println(c == nil)
	b := ref.(*types.Bucket)
	fmt.Println(b == nil)
	return c.PutBucket(*b)
}

type ObjectConverter struct{}

func parseObject(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var o types.Object
	o.BucketName = toString(extractConstant(&remain))
	o.Name = toString(extractConstant(&remain))
	o.VersionId = toString(extractConstant(&remain))
	o.Location = toString(extractConstant(&remain))
	o.Pool = toString(extractConstant(&remain))
	o.OwnerId = toString(extractConstant(&remain))
	sizeStr := toString(extractConstant(&remain))
	o.Size, err = strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return ErrInvalidLine
	}
	o.ObjectId = toString(extractConstant(&remain))
	mTime := toString(extractConstant(&remain))
	o.LastModifiedTime, err = time.Parse(types.TIME_LAYOUT_TIDB, mTime)
	if err != nil {
		return
	}
	o.Etag = toString(extractConstant(&remain))
	o.ContentType = toString(extractConstant(&remain))
	attrs := extractJson(&remain)
	if toString(attrs) == "{}" {
		o.CustomAttributes = nil
		aclStr := extractJson(&remain)
		err = json.Unmarshal(aclStr, &o.ACL)
		if err != nil {
			return fmt.Errorf("json.Unmarshal o.ACL err: %s", err.Error())
		}
	} else if strings.HasPrefix(toString(attrs), "null") {
		// attr is : `null','{"CannedAcl": ""}`
		o.CustomAttributes = nil
		aclStr := attrs[7:]
		err = json.Unmarshal(aclStr, &o.ACL)
		if err != nil {
			return fmt.Errorf("json.Unmarshal o.ACL err: %s", err.Error())
		}
	} else {
		err = json.Unmarshal(attrs, &o.CustomAttributes)
		if err != nil {
			return fmt.Errorf("json.Unmarshal o.CustomAttributes %s err: %s", toString(attrs), err.Error())
		}
		aclStr := extractJson(&remain)
		err = json.Unmarshal(aclStr, &o.ACL)
		if err != nil {
			return fmt.Errorf("json.Unmarshal o.ACL err: %s", err.Error())
		}
	}

	//Ignore nullVersion
	extractConstant(&remain)
	deleteMarker := toString(extractConstant(&remain))
	o.DeleteMarker = deleteMarker == "1"
	o.SseType = toString(extractConstant(&remain))
	o.EncryptionKey = extractConstant(&remain)
	o.InitializationVector = extractConstant(&remain)
	tp := toString(extractConstant(&remain))
	tpN, err := strconv.Atoi(tp)
	if err != nil {
		return fmt.Errorf("strconv.Atoi o.Type %s err: %s", tp, err.Error())
	}
	o.Type = types.ObjectType(tpN)
	storageClass := toString(extractConstant(&remain))
	storageClassN, err := strconv.Atoi(storageClass)
	if err != nil {
		return fmt.Errorf("strconv.Atoi o.StorageClass %s err: %s", storageClass, err.Error())
	}
	o.StorageClass = common.StorageClass(storageClassN)
	cTime := toString(extractConstant(&remain))
	o.CreateTime, err = strconv.ParseUint(cTime, 10, 64)
	if err != nil {
		return err
	}
	*ref.(*types.Object) = o
	return

}

func (_ ObjectConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseObject(dml, ref)
}

func (_ ObjectConverter) Convert(ref interface{}) (err error) {
	o := ref.(*types.Object)
	if o.Type == types.ObjectTypeMultipart {
		v := math.MaxUint64 - o.CreateTime
		partStartKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 0)
		partEndKey := GenTempObjectPartKey(o.BucketName, o.Name, strconv.FormatUint(v, 10), 10000)
		kvs, err := c.TxScan(partStartKey, partEndKey, 10000, nil)
		if err != nil {
			return err
		}
		var parts = make(map[int]*types.Part)
		for _, kv := range kvs {
			var part types.Part
			err = helper.MsgPackUnMarshal(kv.V, &part)
			if err != nil {
				return err
			}
			parts[part.PartNumber] = &part
		}
		o.Parts = parts
	}
	return c.PutObject(o, nil, false)
}

type LifeCycleConverter struct{}

func parseLifeCycle(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var l types.LifeCycle
	l.BucketName = toString(extractConstant(&remain))
	l.Status = toString(extractConstant(&remain))
	startTime := toString(extractConstant(&remain))
	l.StartTime, err = strconv.ParseUint(startTime, 10, 64)
	if err != nil {
		return err
	}
	endTime := toString(extractConstant(&remain))
	l.EndTime, err = strconv.ParseUint(endTime, 10, 64)
	if err != nil {
		return err
	}
	*ref.(*types.LifeCycle) = l
	return
}

func (_ LifeCycleConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseLifeCycle(dml, ref)
}

func (_ LifeCycleConverter) Convert(ref interface{}) (err error) {
	l := ref.(*types.LifeCycle)
	lcKey := tikvclient.GenLifecycleKey(l.BucketName)
	return c.TxPut(lcKey, *l)
}

type MultipartsConverter struct{}

func parseMultiparts(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var m types.Multipart
	m.BucketName = toString(extractConstant(&remain))
	m.ObjectName = toString(extractConstant(&remain))
	// uploadtime
	uploadTimeStr := toString(extractConstant(&remain))
	// NOTE: now, m.InitialTime = math.MaxUint64 - NanoTs
	uploadTime, err := strconv.ParseUint(uploadTimeStr, 10, 64)
	m.InitialTime = math.MaxUint64 - uploadTime
	m.Metadata.InitiatorId = toString(extractConstant(&remain))
	m.Metadata.OwnerId = toString(extractConstant(&remain))
	v, ok := reflectMap[m.Metadata.OwnerId]
	if !ok {
		return ErrNoSuchUser
	}
	m.Metadata.OwnerId = v
	m.Metadata.ContentType = toString(extractConstant(&remain))
	m.Metadata.Location = toString(extractConstant(&remain))
	m.Metadata.Pool = toString(extractConstant(&remain))

	aclStr := extractJson(&remain)
	err = json.Unmarshal(aclStr, &m.Metadata.Acl)
	if err != nil {
		return
	}

	sseStr := extractJson(&remain)
	err = json.Unmarshal(sseStr, &m.Metadata.SseRequest)
	if err != nil {
		return
	}

	m.Metadata.EncryptionKey = extractConstant(&remain)
	m.Metadata.CipherKey = extractConstant(&remain)

	attrStr := extractJson(&remain)
	err = json.Unmarshal(attrStr, &m.Metadata.Attrs)
	if err != nil {
		return
	}

	storageClass := toString(extractConstant(&remain))
	storageClassN, err := strconv.Atoi(storageClass)
	if err != nil {
		return err
	}
	m.Metadata.StorageClass = common.StorageClass(storageClassN)
	// fill in uploadId
	err = m.GenUploadId()
	if err != nil {
		return err
	}
	*ref.(*types.Multipart) = m

	return
}

func (_ MultipartsConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseMultiparts(dml, ref)
}

// multipartpart
type PartsConverter struct{}

func (_ PartsConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseObjectPart(dml, ref)
}

func (_ PartsConverter) Convert(ref interface{}) (err error) {
	p := ref.(*types.Part)
	uploadId := types.GetMultipartUploadIdByDbTime(p.Version)
	partKey := tikvclient.GenObjectPartKey(p.BucketName, p.ObjectName, uploadId, p.PartNumber)
	return c.TxPut(partKey, *p)
}

func (_ MultipartsConverter) Convert(ref interface{}) (err error) {
	m := ref.(*types.Multipart)
	return c.CreateMultipart(*m)
}

type ClusterConverter struct{}

func parseCluster(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	var c types.Cluster
	c.Fsid = toString(extractConstant(&remain))
	c.Pool = toString(extractConstant(&remain))
	weight := toString(extractConstant(&remain))
	c.Weight, err = strconv.Atoi(weight)
	if err != nil {
		return err
	}

	*ref.(*types.Cluster) = c
	return
}

func (_ ClusterConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseCluster(dml, ref)
}

func (_ ClusterConverter) Convert(ref interface{}) (err error) {
	cluster := ref.(*types.Cluster)
	clusterKey := tikvclient.GenClusterKey(cluster.Pool, cluster.Fsid, cluster.Backend)
	return c.TxPut(clusterKey, *cluster)
}

type UserConverter struct{}

func parseUsers(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	var u types.Users
	u.OwnerId = toString(extractConstant(&remain))
	v, ok := reflectMap[u.OwnerId]
	if !ok {
		return ErrNoSuchUser
	}
	u.OwnerId = v
	u.BucketName = toString(extractConstant(&remain))

	*ref.(*types.Users) = u
	return
}

func (_ UserConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseUsers(dml, ref)
}

func (_ UserConverter) Convert(ref interface{}) (err error) {
	u := ref.(*types.Users)
	userKey := GenUserBucketKey(u.OwnerId, u.BucketName)
	return c.TxPut(userKey, *u)
}

type QosConverter struct{}

func parseQos(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	var q types.UserQos
	q.UserID = toString(extractConstant(&remain))
	v, ok := reflectMap[q.UserID]
	if !ok {
		return ErrNoSuchUser
	}
	q.UserID = v
	rQps := toString(extractConstant(&remain))
	q.ReadQps, err = strconv.Atoi(rQps)
	if err != nil {
		return err
	}
	wQps := toString(extractConstant(&remain))
	q.WriteQps, err = strconv.Atoi(wQps)
	if err != nil {
		return err
	}
	bandWidth := toString(extractConstant(&remain))
	q.Bandwidth, err = strconv.Atoi(bandWidth)
	if err != nil {
		return err
	}

	*ref.(*types.UserQos) = q
	return
}

func (_ QosConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseQos(dml, ref)
}

func (_ QosConverter) Convert(ref interface{}) (err error) {
	qos := ref.(*types.UserQos)
	qosKey := tikvclient.GenQoSKey(qos.UserID)
	return c.TxPut(qosKey, *qos)
}

type HotObjectsConverter struct{}

func (_ HotObjectsConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseObject(dml, ref)
}

func (_ HotObjectsConverter) Convert(ref interface{}) (err error) {
	o := ref.(*types.Object)
	return c.AppendObject(o, false)
}

type RestoreConverter struct{}

func parseRestore(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var f types.Freezer
	f.BucketName = toString(extractConstant(&remain))
	f.Name = toString(extractConstant(&remain))
	f.VersionId = toString(extractConstant(&remain))
	statusStr := toString(extractConstant(&remain))
	status, err := strconv.Atoi(statusStr)
	if err != nil {
		return err
	}
	f.Status = common.RestoreStatus(status)

	lifeTime := toString(extractConstant(&remain))
	f.LifeTime, err = strconv.Atoi(lifeTime)
	if err != nil {
		return err
	}

	mTime := toString(extractConstant(&remain))
	f.LastModifiedTime, err = time.Parse(types.TIME_LAYOUT_TIDB, mTime)
	if err != nil {
		return
	}

	f.Location = toString(extractConstant(&remain))
	f.Pool = toString(extractConstant(&remain))
	// Ignore OwnerId
	extractConstant(&remain)
	sizeStr := toString(extractConstant(&remain))
	f.Size, err = strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return err
	}
	f.ObjectId = toString(extractConstant(&remain))
	// Ignore Etag
	extractConstant(&remain)
	typeStr := toString(extractConstant(&remain))
	typeN, err := strconv.Atoi(typeStr)
	if err != nil {
		return err
	}
	f.Type = types.ObjectType(typeN)
	cTime := toString(extractConstant(&remain))
	f.CreateTime, err = strconv.ParseUint(cTime, 10, 64)
	if err != nil {
		return err
	}

	*ref.(*types.Freezer) = f
	return
}

func (_ RestoreConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseRestore(dml, ref)
}

func (_ RestoreConverter) Convert(ref interface{}) (err error) {
	f := ref.(*types.Freezer)
	if f.Type == types.ObjectTypeMultipart {
		v := math.MaxUint64 - f.CreateTime
		partStartKey := GenTempRestoreObjectPartKey(f.BucketName, f.Name, strconv.FormatUint(v, 10), 0)
		partEndKey := GenTempRestoreObjectPartKey(f.BucketName, f.Name, strconv.FormatUint(v, 10), 10000)
		kvs, err := c.TxScan(partStartKey, partEndKey, 10000, nil)
		if err != nil {
			return err
		}
		var parts = make(map[int]*types.Part)
		for _, kv := range kvs {
			var part types.Part
			err = helper.MsgPackUnMarshal(kv.V, &part)
			if err != nil {
				return err
			}
			parts[part.PartNumber] = &part
		}
		f.Parts = parts
	}
	return c.CreateFreezer(f)
}

type ObjectPartConverter struct{}

func parseObjectPart(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	var p types.Part
	partNumStr := toString(extractConstant(&remain))
	p.PartNumber, err = strconv.Atoi(partNumStr)
	if err != nil {
		return err
	}

	sizeStr := toString(extractConstant(&remain))
	p.Size, err = strconv.ParseInt(sizeStr, 10, 64)
	if err != nil {
		return err
	}

	p.ObjectId = toString(extractConstant(&remain))

	offsetStr := toString(extractConstant(&remain))
	p.Offset, err = strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return err
	}

	p.Etag = toString(extractConstant(&remain))
	p.LastModified = toString(extractConstant(&remain))
	p.InitializationVector = extractConstant(&remain)
	p.BucketName = toString(extractConstant(&remain))
	p.ObjectName = toString(extractConstant(&remain))
	versionStr := toString(extractConstant(&remain))
	p.Version, err = strconv.ParseUint(versionStr, 10, 64)
	if err != nil {
		return err
	}

	*ref.(*types.Part) = p
	return
}

func (_ ObjectPartConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseObjectPart(dml, ref)
}

const TableTempObjectPartsPrefix = "op" // for convert

// version = maxInt64 - object.CreateTime
func GenTempObjectPartKey(bucketName, objectName, version string, partNum int) []byte {
	return tikvclient.GenKey(TableTempObjectPartsPrefix, bucketName, objectName, version, fmt.Sprintf("%05d", partNum))
}

// fill in object meta
func (_ ObjectPartConverter) Convert(ref interface{}) (err error) {
	// TODO: Sure Part Version and Time
	p := ref.(*types.Part)
	version := strconv.FormatUint(p.Version, 10)
	key := GenTempObjectPartKey(p.BucketName, p.ObjectName, version, p.PartNumber)
	return c.TxPut(key, *p)
}

type RestoreObjectPartConverter struct{}

func (_ RestoreObjectPartConverter) Parse(dml []byte, ref interface{}) (err error) {
	return parseObjectPart(dml, ref)
}

const TableTempRestoreObjectPartsPrefix = "rp" // for convert

// version = maxInt64 - object.CreateTime
func GenTempRestoreObjectPartKey(bucketName, objectName, version string, partNum int) []byte {
	return tikvclient.GenKey(TableTempRestoreObjectPartsPrefix, bucketName, objectName, version, fmt.Sprintf("%05d", partNum))
}

func (_ RestoreObjectPartConverter) Convert(ref interface{}) (err error) {
	// TODO: Sure Part Version and Time
	p := ref.(*types.Part)
	version := strconv.FormatUint(p.Version, 10)
	key := GenTempRestoreObjectPartKey(p.BucketName, p.ObjectName, version, p.PartNumber)
	return c.TxPut(key, *p)
}

func ConvertByDMLFile(dir, database, table string) {
	t, ok := TableMap[table]
	if !ok {
		fmt.Println("no such table:", table)
	}
	dir = strings.TrimRight(dir, "/")
	for i := 0; ; i++ {
		dmlFilePath := dir + "/" + database + "." + table + "." + strconv.Itoa(i) + ".sql"
		f, err := os.Open(dmlFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				if i == 0 {
					fmt.Println("WARNING:", dmlFilePath, "does not exist.")
				}
				return
			}
			fmt.Println("WARNING:", err, "path:", dmlFilePath)
			return
		}
		defer f.Close()
		fmt.Println("==============" + table + "." + strconv.Itoa(i) + "==============")
		var lineCount int
		rd := bufio.NewReader(f)
		for {
			line, err := rd.ReadBytes('\n')
			if err != nil || io.EOF == err {
				break
			}
			// Skip line 1 of Zone comment and line 2 of 'INSERT INTO VALUES'
			if lineCount < 2 {
				lineCount++
				continue
			}
			line = bytes.TrimSuffix(line, []byte("\n"))
			line = bytes.ReplaceAll(line, []byte("\\"), []byte(""))
			ref := newRef(table)
			err = t.Converter.Parse(line, ref)
			if err != nil {
				if err == ErrInvalidLine {
					fmt.Println("WARNING: invalid line:", string(line), "in", dmlFilePath)
				} else {
					fmt.Println("ERROR:", err, " WHEN parse line", string(line), "in", dmlFilePath)
				}
				continue
			}
			out, _ := json.Marshal(ref)
			fmt.Println(string(out))
			if !global.Verbose {
				err = t.Converter.Convert(ref)
				if err != nil {
					fmt.Println("ERROR:", err, "convert line:", string(line), "in", dmlFilePath, "out:", string(out))
				}
			}
		}
	}
}

func newRef(table string) (ref interface{}) {
	switch table {
	case TableBuckets:
		return new(types.Bucket)
	case TableHotObjects, TableObjects:
		return new(types.Object)
	case TableLifeCycle:
		return new(types.LifeCycle)
	case TableMultiParts:
		return new(types.Multipart)
	case TableQos:
		return new(types.UserQos)
	case TableClusters:
		return new(types.Cluster)
	case TableObjectPart, TableRestoreObjectPart, TableParts:
		return new(types.Part)
	case TableRestore:
		return new(types.Freezer)
	case TableUsers:
		return new(types.Users)
	default:
		return nil
	}
}
