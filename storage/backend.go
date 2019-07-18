package storage

import "io"

type backend interface {
	// get cluster ID
	ClusterID() string
	// put new object to storage backend
	Put(poolName string, object io.Reader) (objectName string,
		bytesWritten uint64, err error)
	// append new chunk to object, existName could be empty
	Append(poolName, existName string, objectChunk io.Reader,
		offset int64) (objectName string, bytesWritten uint64, err error)
	// get a ReadCloser for object
	GetReader(poolName, objectName string,
		offset int64, length uint64) (io.ReadCloser, error)
	// remove an object
	Remove(poolName, objectName string) error
}

// Works together with `wrapAlignedEncryptionReader`, see comments there.
func getAlignedReader(cluster backend, poolName, objectName string,
	startOffset int64, length uint64) (reader io.ReadCloser, err error) {

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += uint64(startOffset - alignedOffset)
	return cluster.GetReader(poolName, objectName, alignedOffset, length)
}