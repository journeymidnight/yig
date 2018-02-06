package types

import (
	"bytes"
	"encoding/binary"
)

type ObjMap struct {
	Rowkey     []byte // Rowkey cache
	Name       string
	BucketName string
	NullVerNum uint64
	NullVerId  string
}

func (om *ObjMap) GetRowKey() (string, error) {
	if len(om.Rowkey) != 0 {
		return string(om.Rowkey), nil
	}
	var rowkey bytes.Buffer
	rowkey.WriteString(om.BucketName + ObjectNameSeparator)

	rowkey.WriteString(om.Name + ObjectNameSeparator)

	om.Rowkey = rowkey.Bytes()
	return string(om.Rowkey), nil
}

func (om *ObjMap) GetValues() (values map[string]map[string][]byte, err error) {
	var nullVerNum bytes.Buffer
	err = binary.Write(&nullVerNum, binary.BigEndian, om.NullVerNum)
	if err != nil {
		return
	}
	values = map[string]map[string][]byte{
		OBJMAP_COLUMN_FAMILY: map[string][]byte{
			"nullVerNum": nullVerNum.Bytes(),
		},
	}
	return
}

func (om *ObjMap) GetValuesForDelete() (values map[string]map[string][]byte) {
	return map[string]map[string][]byte{
		OBJMAP_COLUMN_FAMILY: map[string][]byte{},
	}
}
