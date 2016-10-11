package storage

import (
	"git.letv.cn/yig/yig/helper"
	"git.letv.cn/yig/yig/meta"
	"git.letv.cn/yig/yig/redis"
	"io"
	"io/ioutil"
	"time"
)

const (
	// only objects smaller than threshold are cached
	FILE_CACHE_THRESHOLD_SIZE = 4 << 20 // 4M
)

type DataCache struct {
	failedCacheInvalidOperation chan string
}

func newDataCache() (d *DataCache) {
	d = &DataCache{
		failedCacheInvalidOperation: make(chan string, helper.CONFIG.RedisConnectionNumber),
	}
	go invalidRedisCache(d)
	return d
}

// redo failed invalid operation in DataCache.failedCacheInvalidOperation channel
func invalidRedisCache(d *DataCache) {
	for {
		key := <-d.failedCacheInvalidOperation
		err := redis.Remove(redis.FileTable, key)
		if err != nil {
			d.failedCacheInvalidOperation <- key
			time.Sleep(1 * time.Second)
		}
	}
}

// `writeThrough` performs normal workflow without cache
// `onCacheMiss` should be able to read the WHOLE object
func (d *DataCache) Write(object *meta.Object, startOffset int64, length int64,
	out io.Writer, writeThrough func(io.Writer) error, onCacheMiss func(io.Writer) error) error {

	if object.Size > FILE_CACHE_THRESHOLD_SIZE {
		return writeThrough(out)
	}

	cacheKey := object.BucketName + ":" + object.Name + ":" + object.VersionId

	file, err := redis.GetBytes(cacheKey, startOffset, startOffset+length-1)
	if err == nil && file != nil {
		_, err := out.Write(file)
		return err
	}

	reader, writer := io.Pipe()
	go onCacheMiss(writer)
	o, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	redis.Set(redis.FileTable, cacheKey, o)
	_, err = out.Write(o[startOffset : startOffset+length])
	return err
}

// actually get a `ReadCloser`, aligned to AES_BLOCK_SIZE for encryption
// `readThrough` performs normal workflow without cache
// `onCacheMiss` should be able to read the WHOLE object
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func (d *DataCache) GetAlignedReader(object *meta.Object, startOffset int64, length int64,
	readThrough func() (io.ReadCloser, error),
	onCacheMiss func(io.Writer) error) (io.ReadCloser, error) {

	if object.Size > FILE_CACHE_THRESHOLD_SIZE {
		return readThrough()
	}

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += startOffset - alignedOffset
	startOffset = alignedOffset

	cacheKey := object.BucketName + ":" + object.Name + ":" + object.VersionId

	file, err := redis.GetBytes(cacheKey, startOffset, startOffset+length-1)
	if err == nil && file != nil {
		r := newReadCloser(file)
		return r, nil
	}

	reader, writer := io.Pipe()
	go onCacheMiss(writer)
	file, err = ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	redis.Set(redis.FileTable, cacheKey, file)
	r := newReadCloser(file[startOffset : startOffset+length])
	return r, nil
}

func (d *DataCache) Remove(object *meta.Object) {
	key := object.BucketName + ":" + object.Name + ":" + object.VersionId
	err := redis.Remove(redis.FileTable, key)
	if err != nil {
		d.failedCacheInvalidOperation <- key
	}
}

type ReadCloser struct {
	s []byte
	i int64 // current reading index
}

func (r *ReadCloser) Read(b []byte) (n int, err error) {
	if r.i >= int64(len(r.s)) {
		return 0, io.EOF
	}
	n = copy(b, r.s[r.i:])
	r.i += int64(n)
	return
}

func (r *ReadCloser) Close() error {
	r.s = nil // release memory
	return nil
}

func newReadCloser(b []byte) *ReadCloser {
	return &ReadCloser{b, 0}
}
