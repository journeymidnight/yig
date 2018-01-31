package main

import (
	"context"
	//	"encoding/json"
	//"encoding/hex"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cannium/gohbase"
	"github.com/cannium/gohbase/filter"
	"github.com/cannium/gohbase/hrpc"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	Address = "55c7d702f62e"
	//Address             = "10.72.84.155"
	ObjectNameSeparator = "\n"
	ObjectTable         = "objects"
)

type Object struct {
	Rowkey           string
	BucketName       string
	Location         string // which Ceph cluster this object locates
	Pool             string // which Ceph pool this object locates
	OwnerId          string
	Size             string // file size
	ObjectId         string // object name in Ceph
	LastModifiedTime string // in format "2006-01-02T15:04:05.000Z"
	Etag             string
	Attr             string
	Parts            map[int]*Part
	IV               string
	Acl              string
	ContentType      string
	DeleteMarker     string
	EncryptionKey    string
	NullVersion      string
	SseType          string
}

type Part struct {
	PartNumber int
	Size       int64
	ObjectId   string

	// offset of this part in whole object, calculated when moving parts from
	// `multiparts` table to `objects` table
	Offset               int64
	Etag                 string
	LastModified         string // time string of format "2006-01-02T15:04:05.000Z"
	InitializationVector []byte
}

func main() {
	if len(os.Args) < 4 {
		showUsage()
		return
	}
	switch os.Args[1] {
	case "get":
		singleobject(os.Args[2], os.Args[3])
	default:
		showUsage()
	}
}

func showUsage() {
	fmt.Println("hbase get bucket object")
}

func singleobject(bucket, object string) {
	cli := gohbase.NewClient(Address)
	rowprefix := bucket + ObjectNameSeparator + object
	prefix := filter.NewPrefixFilter([]byte(rowprefix))
	req, err := hrpc.NewScanStr(context.Background(), ObjectTable, hrpc.Filters(prefix), hrpc.NumberOfRows(1))
	if err != nil {
		fmt.Println(err)
	}
	res, err := cli.Scan(req)
	if err != nil {
		fmt.Println(err)
	}
	if len(res) != 1 {
		fmt.Println("empty value found,please check your bucket and object name")
		return
	}
	cells := res[0].Cells
	obj := &Object{}
	obj.Parts = make(map[int]*Part)
	obj.Rowkey = parseRow(cells[0].Row)
	for _, cell := range cells {
		switch string(cell.Family) {
		case "o":
			switch string(cell.Qualifier) {
			case "bucket":
				obj.BucketName = string(cell.Value)
			case "location":
				obj.Location = string(cell.Value)
			case "owner":
				obj.OwnerId = string(cell.Value)
			case "pool":
				obj.Pool = string(cell.Value)
			case "oid":
				obj.ObjectId = string(cell.Value)
			case "size":
				var s int64
				binary.Read(bytes.NewReader(cell.Value), binary.BigEndian, &s)
				obj.Size = strconv.FormatInt(s, 10)
			case "lastModified":
				obj.LastModifiedTime = string(cell.Value)
			case "etag":
				obj.Etag = string(cell.Value)
			case "attributes":
				obj.Attr = string(cell.Value)
			case "IV":
				obj.IV = string(cell.Value)
			case "ACL":
				obj.Acl = string(cell.Value)
			case "content-type":
				obj.ContentType = string(cell.Value)
			case "deleteMarker":
				obj.DeleteMarker = string(cell.Value)
			case "encryptionKey":
				obj.EncryptionKey = string(cell.Value)
			case "nullVersion":
				obj.NullVersion = string(cell.Value)
			case "sseType":
				obj.SseType = string(cell.Value)
			default:
				fmt.Println("skip", string(cell.Qualifier), "value:", string(cell.Value))
			}
		case "p":
			var partNumber int
			partNumber, err = strconv.Atoi(string(cell.Qualifier))
			if err != nil {
				return
			}
			var p Part
			err = json.Unmarshal(cell.Value, &p)
			if err != nil {
				return
			}
			obj.Parts[partNumber] = &p
		}
	}
	display(obj)
	showParts(obj)
	//	fmt.Println("cell:{", parseRow(cell.Row), "=", string(cell.Family), "=", string(cell.Qualifier), "=", string(cell.Value), "}")
}

func display(obj *Object) {
	fmt.Printf("|%-36s|%-8s|%-40s|%-8s|%-12s|%-28s|%-10s|%-36s|%-8s|%-8s|%-8s|%-30s|%-11s|%-8s|%-11s|%-8s|\n", "ROWKEY", "BUCKET", "LOCATION", "POOL", "OID", "MODIFIED", "SIZE", "ETAG", "ATTR", "IV", "ACL", "CONTENT-TYPE", "DELETEMAKER", "ENCRYKEY", "NULLVERSION", "SSETYPE")
	fmt.Printf("|%-36s|%-8s|%-40s|%-8s|%-12s|%-28s|%-10s|%-36s|%-8s|%-8s|%-8s|%-30s|%-11s|%-8s|%-11s|%-8s|\n", obj.Rowkey, obj.BucketName, obj.Location, obj.Pool, obj.ObjectId, obj.LastModifiedTime, obj.Size, obj.Etag, obj.Attr, obj.IV, obj.Acl, obj.ContentType, obj.DeleteMarker, obj.EncryptionKey, obj.NullVersion, obj.SseType)
}

func showParts(obj *Object) {
	fmt.Println("\n Display part detail below:")
	for _, part := range obj.Parts {
		//fmt.Printf("|%-16d|%-16s|%-16s|%-16d|%-16s|\n", num, part.location, part.pool, part.Size, part.etag)
		//fmt.Println(num, part)
		a, e := json.Marshal(part)
		fmt.Println(string(a), e)
	}
}

func parseRow(row []byte) string {
	length := len(row)
	name := string(row[:length-8])
	t := row[length-8 : length]
	var a uint64
	binary.Read(bytes.NewReader(t), binary.BigEndian, &a)
	//	checktime(a)
	all := name + strconv.FormatUint(a, 10)
	nall := strings.Join(strings.Split(all, "\n"), "\\n")
	return nall
}

//used for debug createtime
func checktime(num uint64) {
	t := math.MaxUint64 - num
	//	tstr := strconv.FormatUint(t, 10)
	//	fmt.Println("timestamp is", int64(t))
	l := time.Unix(int64(t)/int64(time.Second), 0).Format("2006-01-02 15:04:05")
	//	l, e := te("2006-01-02 15:04:05", tstr)
	fmt.Println(l)
}
