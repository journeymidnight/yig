package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/meta/common"
	"github.com/journeymidnight/yig/meta/types"
)

type ParseFn func(dml []byte, ref interface{}) (err error)

const DefaultDatabase = "yig"

var ErrInvalidLine = fmt.Errorf("invalid line.")

type Parser struct {
	ref     interface{}
	parseFn ParseFn
}

func ParseDMLFile(dir, database, table string) (err error) {
	p, err := GetParser(table)
	if err != nil {
		return
	}
	dir = strings.TrimRight(dir, "/")
	dmlFilePath := dir + "/" + database + "." + table + ".sql"
	f, err := os.Open(dmlFilePath)
	if os.IsNotExist(err) {
		fmt.Println("WARNING:", dmlFilePath, "does not exist.")
		return nil
	} else if err != nil {
		return err
	}
	defer f.Close()
	rd := bufio.NewReader(f)
	for {
		line, err := rd.ReadBytes('\n')
		if err != nil || io.EOF == err {
			break
		}
		err = p.parseFn(line, p.ref)
		if err != nil {
			if err == ErrInvalidLine {
				fmt.Println("WARNING: invalid line:", line, "in", dmlFilePath)
			} else {
				fmt.Println("ERROR:", err, "line", line, "in", dmlFilePath)
			}
			return err
		}
	}
	return nil
}

func GetParser(table string) (p Parser, err error) {
	t, ok := TableMap[table]
	if !ok {
		return p, fmt.Errorf("No such table.")
	}
	p.parseFn = t.ParseFn
	switch table {
	case TableBuckets:
		p.ref = new(types.Bucket)
	case TableHotObjects:
		fallthrough
	case TableObjects:
		p.ref = new(types.Object)
	case TableLifeCycle:
		p.ref = new(types.LifeCycle)
	case TableMultiParts:
		p.ref = new(types.Multipart)
	case TableQos:
		p.ref = new(types.UserQos)
	case TableClusters:
		p.ref = new(types.Cluster)
	case TableParts:
		fallthrough
	case TableObjectPart:
		p.ref = new(types.Part)
	case TableRestore:
		p.ref = new(types.Freezer)
	}
	return
}

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
	b.OwnerId = reflectMap[b.OwnerId]

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
	if toString(attrs) == "null" || toString(attrs) == "{}" {
		o.CustomAttributes = nil
	} else {
		err = json.Unmarshal(attrs, &o.CustomAttributes)
		if err != nil {
			return
		}
	}

	aclStr := extractJson(&remain)
	err = json.Unmarshal(aclStr, &o.ACL)
	if err != nil {
		return
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
		return err
	}
	o.Type = types.ObjectType(tpN)
	storageClass := toString(extractConstant(&remain))
	storageClassN, err := strconv.Atoi(storageClass)
	if err != nil {
		return err
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

// TODO: add test
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

// TODO: add test
func parseMultiparts(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}
	var m types.Multipart
	m.BucketName = toString(extractConstant(&remain))
	m.ObjectName = toString(extractConstant(&remain))
	// uploadtime
	iTime := toString(extractConstant(&remain))
	// NOTE: now, m.InitialTime = math.MaxUint64 - NanoTs
	m.InitialTime, err = strconv.ParseUint(iTime, 10, 64)
	m.Metadata.InitiatorId = toString(extractConstant(&remain))
	m.Metadata.OwnerId = toString(extractConstant(&remain))
	m.Metadata.OwnerId = reflectMap[m.Metadata.OwnerId]
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

	*ref.(*types.Multipart) = m

	return
}

// TODO: add test
func parseQos(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	var q types.UserQos
	q.UserID = toString(extractConstant(&remain))
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

// TODO: add test
func parseClusters(dml []byte, ref interface{}) (err error) {
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

// TODO: add test
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

// TODO: add test
func parseUsers(dml []byte, ref interface{}) (err error) {
	var remain = tidyDml(dml)
	if remain == nil {
		return ErrInvalidLine
	}

	return
}

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
