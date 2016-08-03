package meta

import (
	"bytes"
	"encoding/binary"
	"strings"
	"time"
)

// Rowkey format:
// BucketName +
// bigEndian(uint16(count("/", ObjectName))) +
// ObjectName +
// bigEndian(uint64.max - unixNanoTimestamp)
// The prefix excludes timestamp part
func GetObjectRowkeyPrefix(bucketName string, objectName string) ([]byte, error) {
	var rowkey bytes.Buffer
	rowkey.WriteString(bucketName)
	err := binary.Write(&rowkey, binary.BigEndian, uint16(strings.Count(objectName, "/")))
	if err != nil {
		return []byte{}, err
	}
	rowkey.WriteString(objectName)
	return rowkey.Bytes(), nil
}

// Rowkey format:
// bigEndian(unixNanoTimestamp) + BucketName + ObjectName
func GetGarbageCollectionRowkey(bucketName string, objectName string) (string, error) {
	var rowkey bytes.Buffer
	err := binary.Write(&rowkey, binary.BigEndian,
		uint64(time.Now().UnixNano()))
	if err != nil {
		return "", err
	}
	rowkey.WriteString(bucketName)
	rowkey.WriteString(objectName)
	return rowkey.String(), nil
}
