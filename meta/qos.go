package meta

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/journeymidnight/yig/redis"

	"github.com/go-redis/redis_rate/v8"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/client"
	"github.com/journeymidnight/yig/meta/types"
)

type QosMeta struct {
	client      client.Client
	rateLimiter *redis_rate.Limiter

	// Not using a mutex to protect bucketUser or userQosLimit,
	// since it's acceptable to read stale or empty values.
	// bucket name -> user id
	bucketUser map[string]string
	// user id -> user qos limit
	userQosLimit map[string]types.UserQos

	// extra tokens consumed when reader/writer is closed
	globalRefill int64
}

func NewQosMeta(client client.Client) *QosMeta {
	m := &QosMeta{
		client:      client,
		rateLimiter: redis.QosLimiter,
	}
	go m.inMemoryCacheSync()
	return m
}

func (m *QosMeta) AllowReadQuery(bucketName string) (allow bool) {
	userID := m.bucketUser[bucketName]
	qps := m.userQosLimit[userID].ReadQps
	if qps <= 0 {
		qps = helper.CONFIG.DefaultReadOps
	}
	key := fmt.Sprintf("user_rqps_%s", userID)
	// the key actually used in redis would have a prefix "rate:"
	result, err := m.rateLimiter.Allow(key, redis_rate.PerSecond(qps))
	if err != nil {
		return true
	}
	return result.Allowed
}

func (m *QosMeta) AllowWriteQuery(bucketName string) (allow bool) {
	userID := m.bucketUser[bucketName]
	qps := m.userQosLimit[userID].WriteQps
	if qps <= 0 {
		qps = helper.CONFIG.DefaultWriteOps
	}
	key := fmt.Sprintf("user_wqps_%s", userID)
	// the key actually used in redis would have a prefix "rate:"
	result, err := m.rateLimiter.Allow(key, redis_rate.PerSecond(qps))
	if err != nil {
		return true
	}
	return result.Allowed
}

func (m *QosMeta) newThrottler(bucketName string, defaultBufferSize int64) throttler {
	userID := m.bucketUser[bucketName]
	bandwidthKBps := m.userQosLimit[userID].Bandwidth
	if bandwidthKBps <= 0 {
		bandwidthKBps = helper.CONFIG.DefaultBandwidthKBps
	}
	t := throttler{
		rateLimiter:  m.rateLimiter,
		userID:       userID,
		kbpsLimit:    bandwidthKBps,
		globalRefill: &m.globalRefill,
	}
	if m.globalRefill >= defaultBufferSize {
		atomic.AddInt64(&m.globalRefill, -defaultBufferSize)
		t.refill = int(defaultBufferSize) // buffer size should fit int32
	}
	return t
}

func (m *QosMeta) NewThrottleReader(bucketName string, reader io.Reader) *ThrottleReader {
	// in yig, upload requests use "reader"
	if !helper.CONFIG.EnableQoS {
		return &ThrottleReader{
			reader: reader,
			t:      nil,
		}
	}
	throttle := m.newThrottler(bucketName, helper.CONFIG.UploadMaxChunkSize)
	return &ThrottleReader{
		reader: reader,
		t:      &throttle,
	}
}

func (m *QosMeta) NewThrottleWriter(bucketName string, writer io.Writer) *ThrottleWriter {
	// in yig, download requests use "writer"
	if !helper.CONFIG.EnableQoS {
		return &ThrottleWriter{
			writer: writer,
			t:      nil,
		}
	}
	throttle := m.newThrottler(bucketName, helper.CONFIG.DownloadBufPoolSize)
	return &ThrottleWriter{
		writer: writer,
		t:      &throttle,
	}
}

// I believe it's OK to load all data into memory when user count < 10k, so...
func (m *QosMeta) inMemoryCacheSync() {
	for {
		bucketUser, err := m.client.GetAllUserBuckets()
		if err != nil {
			helper.Logger.Error("GetAllUserBuckets error:", err)
			bucketUser = nil
		}
		userQosLimit, err := m.client.GetAllUserQos()
		if err != nil {
			helper.Logger.Error("GetAllUserQos error:", err)
			userQosLimit = nil
		}
		if bucketUser != nil {
			m.bucketUser = bucketUser
		}
		if userQosLimit != nil {
			m.userQosLimit = userQosLimit
		}

		time.Sleep(10 * time.Minute)
	}
}

type throttler struct {
	rateLimiter  *redis_rate.Limiter
	userID       string
	kbpsLimit    int    // KBps
	refill       int    // extra tokens consumed
	globalRefill *int64 // pointer to QosMeta's globalRefill
}

// Note by test, if 1024 * kbpsLimit < n, which is rare,
// speed would always be 0, i.e. maybeWaitTokenN() would block forever
func (t *throttler) maybeWaitTokenN(n int) {
	key := fmt.Sprintf("user_bandwidth_%s", t.userID)
	if t.refill >= n {
		t.refill -= n
		return
	}
	n -= t.refill
	t.refill = 0
	for {
		result, err := t.rateLimiter.AllowN(key,
			redis_rate.PerSecond(t.kbpsLimit*1024), n)
		if err != nil {
			return
		}
		if result.Allowed {
			return
		}
		time.Sleep(result.RetryAfter)
	}
}

func (t *throttler) Close() {
	if t.refill > 0 {
		atomic.AddInt64(t.globalRefill, int64(t.refill))
		t.refill = 0
	}
}

type ThrottleReader struct {
	reader io.Reader
	t      *throttler
}

func (r *ThrottleReader) Read(p []byte) (int, error) {
	if r.t == nil {
		return r.Read(p)
	}
	r.t.maybeWaitTokenN(len(p))
	n, err := r.reader.Read(p)
	// we consumed len(p) tokens, but transferred n bytes
	r.t.refill += len(p) - n
	return n, err
}

func (r *ThrottleReader) Close() {
	if r.t == nil {
		return
	}
	r.t.Close()
}

type ThrottleWriter struct {
	writer io.Writer
	t      *throttler
}

func (w *ThrottleWriter) Write(p []byte) (int, error) {
	if w.t == nil {
		return w.Write(p)
	}
	w.t.maybeWaitTokenN(len(p))
	n, err := w.writer.Write(p)
	// we consumed len(p) tokens, but transferred n bytes
	w.t.refill += len(p) - n
	return n, err
}

func (w *ThrottleWriter) Close() {
	if w.t == nil {
		return
	}
	w.t.Close()
}
