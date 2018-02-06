package hbaseclient

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"github.com/cannium/gohbase/hrpc"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
	. "github.com/journeymidnight/yig/meta/types"
	"github.com/xxtea/xxtea-go/xxtea"
	"strconv"
)

func (h *HbaseClient) GetObjectMap(bucketName, objectName string) (objMap *ObjMap, err error) {
	objMapRowkeyPrefix, err := getObjectRowkeyPrefix(bucketName, objectName, "")
	if err != nil {
		return
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	getRequest, err := hrpc.NewGetStr(ctx, OBJMAP_TABLE, string(objMapRowkeyPrefix))
	if err != nil {
		return
	}
	getResponse, err := h.Client.Get(getRequest)
	if err != nil {
		return
	}
	if len(getResponse.Cells) == 0 {
		err = ErrNoSuchKey
		return
	}
	objMap, err = ObjMapFromResponse(getResponse)
	return
}

func (h *HbaseClient) PutObjectMap(objMap *ObjMap) error {
	rowkey, err := objMap.GetRowKey()
	if err != nil {
		return err
	}
	values, err := objMap.GetValues()
	if err != nil {
		return err
	}
	helper.Debugln("values", values)
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	put, err := hrpc.NewPutStr(ctx, OBJMAP_TABLE, rowkey, values)
	if err != nil {
		return err
	}
	_, err = h.Client.Put(put)
	return err
}

func (h *HbaseClient) DeleteObjectMap(objMap *ObjMap) error {
	rowkeyToDelete, err := objMap.GetRowKey()
	if err != nil {
		return err
	}
	ctx, done := context.WithTimeout(RootContext, helper.CONFIG.HbaseTimeout)
	defer done()
	deleteRequest, err := hrpc.NewDelStr(ctx, OBJMAP_TABLE, rowkeyToDelete,
		objMap.GetValuesForDelete())
	if err != nil {
		return err
	}
	_, err = h.Client.Delete(deleteRequest)

	return err
}

//util func
func ObjMapFromResponse(response *hrpc.Result) (objMap *ObjMap, err error) {
	objMap = new(ObjMap)
	for _, cell := range response.Cells {
		switch string(cell.Family) {
		case OBJMAP_COLUMN_FAMILY:
			switch string(cell.Qualifier) {
			case "nullVerNum":
				err = binary.Read(bytes.NewReader(cell.Value), binary.BigEndian,
					&objMap.NullVerNum)
				if err != nil {
					return
				}
			}
		}
	}
	timeData := []byte(strconv.FormatUint(objMap.NullVerNum, 10))
	objMap.NullVerId = hex.EncodeToString(xxtea.Encrypt(timeData, XXTEA_KEY))
	//helper.Debugln("ObjectFromResponse:", objMap)
	return
}
