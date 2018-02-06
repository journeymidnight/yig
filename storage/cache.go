package storage

import (
	"io"
	"time"

	"github.com/journeymidnight/yig/helper"
	meta "github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/redis"
	"bytes"
)

const (
	// only objects smaller than threshold are cached
	FILE_CACHE_THRESHOLD_SIZE = 4 << 20 // 4M
)

type DataCache interface {
	WriteFromCache(object *meta.Object, startOffset int64, length int64,
		out io.Writer, writeThrough func(io.Writer) error,
		onCacheMiss func(io.Writer) error) error
	GetAlignedReader(object *meta.Object, startOffset int64, length int64,
		readThrough func() (io.ReadCloser, error),
		onCacheMiss func(io.Writer) error) (io.ReadCloser, error)
	Remove(key string)
}

type enabledDataCache struct {
	failedCacheInvalidOperation chan string
}

type disabledDataCache struct{}

func newDataCache(cacheEnabled bool) (d DataCache) {
	if cacheEnabled {
		d := &enabledDataCache{
			failedCacheInvalidOperation: make(chan string, helper.CONFIG.RedisConnectionNumber),
		}
		go invalidRedisCache(d)
		return d
	}

	return &disabledDataCache{}
}

// redo failed invalid operation in enabledDataCache.failedCacheInvalidOperation channel
func invalidRedisCache(d *enabledDataCache) {
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
func (d *enabledDataCache) WriteFromCache(object *meta.Object, startOffset int64, length int64,
	out io.Writer, writeThrough func(io.Writer) error, onCacheMiss func(io.Writer) error) error {

	if object.Size > FILE_CACHE_THRESHOLD_SIZE {
		return writeThrough(out)
	}

	cacheKey := object.BucketName + ":" + object.Name + ":" + object.GetVersionId()

	file, err := redis.GetBytes(cacheKey, startOffset, startOffset+length-1)
	if err == nil && file != nil && int64(len(file)) == length {
		helper.Debugln("File cache HIT")
		_, err := out.Write(file)
		return err
	}

	helper.Debugln("File cache MISS")

	var buffer bytes.Buffer
	onCacheMiss(&buffer)

	redis.SetBytes(cacheKey, buffer.Bytes())
	_, err = out.Write(buffer.Bytes()[startOffset : startOffset+length])
	return err
}

func (d *disabledDataCache) WriteFromCache(object *meta.Object, startOffset int64, length int64,
	out io.Writer, writeThrough func(io.Writer) error, onCacheMiss func(io.Writer) error) error {

	return writeThrough(out)
}

// actually get a `ReadCloser`, aligned to AES_BLOCK_SIZE for encryption
// `readThrough` performs normal workflow without cache
// `onCacheMiss` should be able to read the WHOLE object
// FIXME: this API causes an extra memory copy, need to patch radix to fix it
func (d *enabledDataCache) GetAlignedReader(object *meta.Object, startOffset int64, length int64,
	readThrough func() (io.ReadCloser, error),
	onCacheMiss func(io.Writer) error) (io.ReadCloser, error) {

	if object.Size > FILE_CACHE_THRESHOLD_SIZE {
		return readThrough()
	}

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += startOffset - alignedOffset
	startOffset = alignedOffset

	cacheKey := object.BucketName + ":" + object.Name + ":" + object.GetVersionId()

	file, err := redis.GetBytes(cacheKey, startOffset, startOffset+length-1)
	if err == nil && file != nil && int64(len(file)) == length {
		helper.Debugln("File cache HIT")
		r := newReadCloser(file)
		return r, nil
	}

	helper.Debugln("File cache MISS")

	var buffer bytes.Buffer
	onCacheMiss(&buffer)

	redis.SetBytes(cacheKey, buffer.Bytes())
	r := newReadCloser(buffer.Bytes()[startOffset : startOffset+length])
	return r, nil
}

func (d *disabledDataCache) GetAlignedReader(object *meta.Object, startOffset int64, length int64,
	readThrough func() (io.ReadCloser, error),
	onCacheMiss func(io.Writer) error) (io.ReadCloser, error) {

	return readThrough()
}

func (d *enabledDataCache) Remove(key string) {
	err := redis.Remove(redis.FileTable, key)
	if err != nil {
		d.failedCacheInvalidOperation <- key
	}
}

func (d *disabledDataCache) Remove(key string) {
	return
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
