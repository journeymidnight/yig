package ceph

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/journeymidnight/radoshttpd/rados"
	"github.com/journeymidnight/yig/backend"
	"github.com/journeymidnight/yig/helper"
	"io"
	"io/ioutil"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	MON_TIMEOUT                = "10"
	OSD_TIMEOUT                = "10"
	STRIPE_UNIT                = 512 << 10 /* 512K */
	STRIPE_COUNT               = 2
	OBJECT_SIZE                = 8 << 20 /* 8M */
	AIO_CONCURRENT             = 4
	DEFAULT_CEPHCONFIG_PATTERN = "conf/*.conf"
	MIN_CHUNK_SIZE             = 512 << 10       // 512K
	BUFFER_SIZE                = 1 << 20         // 1M
	MAX_CHUNK_SIZE             = 8 * BUFFER_SIZE // 8M
)

func Initialize(config helper.Config) map[string]backend.Cluster {
	cephConfigPattern := config.CephConfigPattern
	if cephConfigPattern == "" {
		cephConfigPattern = DEFAULT_CEPHCONFIG_PATTERN
	}
	cephConfigFiles, err := filepath.Glob(cephConfigPattern)
	if err != nil || len(cephConfigFiles) == 0 {
		panic("No ceph conf found")
	}
	helper.Logger.Info("Loading Ceph file:", cephConfigFiles)

	clusters := make(map[string]backend.Cluster)
	for _, conf := range cephConfigFiles {
		c := NewCephStorage(conf)
		clusters[c.Name] = c
	}

	return clusters
}

type CephCluster struct {
	Name       string
	Conn       RadosConn
	InstanceId uint64
	counter    uint64
}

func NewCephStorage(configFile string) *CephCluster {

	helper.Logger.Info("Loading Ceph file", configFile)

	conn, err := rados.NewConn("admin")
	conn.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	conn.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)

	err = conn.ReadConfigFile(configFile)
	if err != nil {
		helper.Logger.Error("Failed to open ceph.conf:", configFile)
		return nil
	}

	err = conn.Connect()
	if err != nil {
		helper.Logger.Error("Failed to connect to remote cluster:", configFile)
		return nil
	}

	name, err := conn.GetFSID()
	if err != nil {
		helper.Logger.Error("Failed to get FSID:", configFile)
		conn.Shutdown()
		return nil
	}

	id := conn.GetInstanceID()

	cluster := CephCluster{
		Conn:       radosConn{conn},
		Name:       name,
		InstanceId: id,
	}

	helper.Logger.Info("Ceph Cluster", name, "is ready, InstanceId is", name, id)
	return &cluster
}

func setStripeLayout(p StriperPool) int {
	var ret int = 0
	if ret = p.SetLayoutStripeUnit(STRIPE_UNIT); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutObjectSize(OBJECT_SIZE); ret < 0 {
		return ret
	}
	if ret = p.SetLayoutStripeCount(STRIPE_COUNT); ret < 0 {
		return ret
	}
	return ret
}

func pending_has_completed(p *list.List) bool {
	if p.Len() == 0 {
		return false
	}
	e := p.Front()
	c := e.Value.(AioCompletion)
	ret := c.IsComplete()
	if ret == 0 {
		return false
	} else {
		return true
	}
}

func wait_pending_front(p *list.List) int {
	/* remove AioCompletion from list */
	e := p.Front()
	p.Remove(e)
	c := e.Value.(AioCompletion)
	c.WaitForComplete()
	ret := c.GetReturnValue()
	c.Release()
	return ret
}

func drain_pending(p *list.List) int {
	var ret int
	for p.Len() > 0 {
		ret = wait_pending_front(p)
	}
	return ret
}

func (cluster *CephCluster) getUniqUploadName() string {
	v := atomic.AddUint64(&cluster.counter, 1)
	oid := fmt.Sprintf("%d:%d", cluster.InstanceId, v)
	return oid
}

func (cluster *CephCluster) Shutdown() {
	cluster.Conn.Shutdown()
}

func (cluster *CephCluster) doSmallPut(poolname string, oid string, data io.Reader) (size uint64, err error) {
	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return 0, errors.New("Bad poolname")
	}
	defer pool.Destroy()

	buf, err := ioutil.ReadAll(data)
	size = uint64(len(buf))
	if err != nil {
		return 0, errors.New("Read from client failed")
	}
	err = pool.WriteSmallObject(oid, buf)
	if err != nil {
		return 0, err
	}

	return size, nil
}

type RadosSmallDownloader struct {
	oid       string
	offset    int64
	remaining int64
	pool      Pool
}

func (rd *RadosSmallDownloader) Read(p []byte) (n int, err error) {
	if rd.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > rd.remaining {
		p = p[:rd.remaining]
	}
	count, err := rd.pool.Read(rd.oid, p, uint64(rd.offset))
	if count == 0 {
		return 0, io.EOF
	}
	rd.offset += int64(count)
	rd.remaining -= int64(count)
	return count, err
}

func (rd *RadosSmallDownloader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		rd.offset = offset
	case 1:
		rd.offset += offset
	case 2:
		panic("Not implemented")
	}
	return rd.offset, nil
}

func (rd *RadosSmallDownloader) Close() error {
	rd.pool.Destroy()
	return nil
}

func (cluster *CephCluster) Put(poolname string, data io.Reader) (oid string,
	size uint64, err error) {

	oid = cluster.getUniqUploadName()
	if poolname == backend.SMALL_FILE_POOLNAME {
		size, err = cluster.doSmallPut(poolname, oid, data)
		return oid, size, err
	}

	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return oid, 0, fmt.Errorf("Bad poolname %s", poolname)
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return oid, 0, fmt.Errorf("Bad ioctx of pool %s", poolname)
	}
	defer striper.Destroy()

	setStripeLayout(striper)

	/* if the data len in pending_data is bigger than current_upload_window, I will flush the data to ceph */
	/* current_upload_window could not dynamically increase or shrink */

	var c AioCompletion
	pending := list.New()
	var current_upload_window = helper.CONFIG.UploadMinChunkSize /* initial window size as MIN_CHUNK_SIZE, max size is MAX_CHUNK_SIZE */
	var pending_data = make([]byte, current_upload_window)

	var slice_offset = 0
	var slow_count = 0
	// slice is the buffer size of reader, the size is equal to remain size of pending_data
	var slice = pending_data[0:current_upload_window]

	var offset uint64 = 0

	for {
		start := time.Now()
		count, err := data.Read(slice)
		if err != nil && err != io.EOF {
			drain_pending(pending)
			return oid, 0,
				fmt.Errorf("Read from client failed. pool:%s oid:%s", poolname, oid)
		}
		if count == 0 {
			break
		}
		// it's used to calculate next upload window
		elapsed_time := time.Since(start)

		slice_offset += count
		slice = pending_data[slice_offset:]

		//is pending_data full?
		if slice_offset < len(pending_data) {
			continue
		}

		/* pending data is full now */
		c, err = striper.WriteAIO(oid, pending_data, offset)
		if err != nil {
			c.Release()
			drain_pending(pending)
			return oid, 0,
				fmt.Errorf("Bad io. pool:%s oid:%s", poolname, oid)
		}
		pending.PushBack(c)

		for pending_has_completed(pending) {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return oid, 0,
					fmt.Errorf("Error drain_pending in pending_has_completed. pool:%s oid:%s", poolname, oid)
			}
		}

		if pending.Len() > AIO_CONCURRENT {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return oid, 0,
					fmt.Errorf("Error wait_pending_front. pool:%s oid:%s", poolname, oid)
			}
		}
		offset += uint64(len(pending_data))

		/* Resize current upload window */
		expected_time := int64(count) * 1000 * 1000 * 1000 / current_upload_window /* 1000 * 1000 * 1000 means use Nanoseconds */

		// If the upload speed is less than half of the current upload window, reduce the upload window by half.
		// If upload speed is larger than current window size per second, used the larger window and twice
		if elapsed_time.Nanoseconds() > 2*int64(expected_time) {
			if slow_count > 2 && current_upload_window > helper.CONFIG.UploadMinChunkSize {
				current_upload_window = current_upload_window >> 1
				slow_count = 0
			}
			slow_count += 1
		} else if int64(expected_time) > elapsed_time.Nanoseconds() {
			/* if upload speed is fast enough, enlarge the current_upload_window a bit */
			current_upload_window = current_upload_window << 1
			if current_upload_window > helper.CONFIG.UploadMaxChunkSize {
				current_upload_window = helper.CONFIG.UploadMaxChunkSize
			}
			slow_count = 0
		}
		/* allocate a new pending data */
		pending_data = make([]byte, current_upload_window)
		slice_offset = 0
		slice = pending_data[0:current_upload_window]
	}

	size = uint64(slice_offset) + offset
	//write all remaining data
	if slice_offset > 0 {
		c, err = striper.WriteAIO(oid, pending_data[:slice_offset], offset)
		if err != nil {
			c.Release()
			return oid, 0, fmt.Errorf("error writing remaining data, pool:%s oid:%s",
				poolname, oid)
		}
		pending.PushBack(c)
	}

	//drain_pending
	if ret := drain_pending(pending); ret < 0 {
		return oid, 0,
			fmt.Errorf("Error wait_pending_front. pool:%s oid:%s", poolname, oid)
	}
	return oid, size, nil
}

func (cluster *CephCluster) Append(poolname string, existName string, data io.Reader,
	offset int64) (oid string, size uint64, err error) {

	oid = existName
	if len(oid) == 0 {
		oid = cluster.getUniqUploadName()
	}
	if poolname != backend.BIG_FILE_POOLNAME {
		return oid, 0,
			errors.New("specified pool must be used for storing big file.")
	}

	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return oid, 0,
			fmt.Errorf("Bad poolname %s", poolname)
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return oid, 0,
			fmt.Errorf("Bad ioctx of pool %s", poolname)
	}
	defer striper.Destroy()

	setStripeLayout(striper)

	var current_upload_window = helper.CONFIG.UploadMinChunkSize /* initial window size as MIN_CHUNK_SIZE, max size is MAX_CHUNK_SIZE */
	var pending_data = make([]byte, current_upload_window)

	var origin_offset = offset
	var slice_offset = 0
	var slow_count = 0
	// slice is the buffer size of reader, the size is equal to remain size of pending_data
	var slice = pending_data[0:current_upload_window]
	for {
		start := time.Now()
		count, err := data.Read(slice)

		if count == 0 {
			break
		}
		// it's used to calculate next upload window
		elapsed_time := time.Since(start)

		slice_offset += count
		slice = pending_data[slice_offset:]

		//is pending_data full?
		if slice_offset < len(pending_data) {
			continue
		}

		/* pending data is full now */
		_, err = striper.Write(oid, pending_data, uint64(offset))
		if err != nil {
			return oid, 0,
				fmt.Errorf("Bad io. pool:%s oid:%s", poolname, oid)
		}

		offset += int64(len(pending_data))

		/* Resize current upload window */
		expected_time := int64(count) * 1000 * 1000 * 1000 / current_upload_window /* 1000 * 1000 * 1000 means use Nanoseconds */

		// If the upload speed is less than half of the current upload window, reduce the upload window by half.
		// If upload speed is larger than current window size per second, used the larger window and twice
		if elapsed_time.Nanoseconds() > 2*expected_time {
			if slow_count > 2 && current_upload_window > helper.CONFIG.UploadMinChunkSize {
				current_upload_window = current_upload_window >> 1
				slow_count = 0
			}
			slow_count += 1
		} else if expected_time > elapsed_time.Nanoseconds() {
			/* if upload speed is fast enough, enlarge the current_upload_window a bit */
			current_upload_window = current_upload_window << 1
			if current_upload_window > helper.CONFIG.UploadMaxChunkSize {
				current_upload_window = helper.CONFIG.UploadMaxChunkSize
			}
			slow_count = 0
		}
		/* allocate a new pending data */
		pending_data = make([]byte, current_upload_window)
		slice_offset = 0
		slice = pending_data[0:current_upload_window]
	}

	size = uint64(int64(slice_offset) + offset - origin_offset)
	//write all remaining data
	if slice_offset > 0 {
		_, err = striper.Write(oid, pending_data, uint64(offset))
		if err != nil {
			return oid, 0,
				fmt.Errorf("Bad io. pool:%s oid:%s", poolname, oid)
		}
	}

	return oid, size, nil
}

type RadosDownloader struct {
	striper   StriperPool
	oid       string
	offset    int64
	remaining int64
	pool      Pool
}

func (rd *RadosDownloader) Read(p []byte) (n int, err error) {
	if rd.remaining <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > rd.remaining {
		p = p[:rd.remaining]
	}
	count, err := rd.striper.Read(rd.oid, p, uint64(rd.offset))
	if count == 0 {
		return 0, io.EOF
	}
	rd.offset += int64(count)
	rd.remaining -= int64(count)
	return count, err
}

func (rd *RadosDownloader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		rd.offset = offset
	case 1:
		rd.offset += offset
	case 2:
		panic("Not implemented")
	}
	return rd.offset, nil
}

func (rd *RadosDownloader) Close() error {
	rd.striper.Destroy()
	rd.pool.Destroy()
	return nil
}

func (cluster *CephCluster) GetReader(poolName string, oid string, startOffset int64,
	length uint64) (reader io.ReadCloser, err error) {

	if poolName == backend.SMALL_FILE_POOLNAME {
		pool, e := cluster.Conn.OpenPool(poolName)
		if e != nil {
			err = errors.New("bad poolname")
			return
		}
		radosSmallReader := &RadosSmallDownloader{
			oid:       oid,
			offset:    startOffset,
			pool:      pool,
			remaining: int64(length),
		}

		return radosSmallReader, nil
	}

	pool, err := cluster.Conn.OpenPool(poolName)
	if err != nil {
		err = errors.New("bad poolname")
		return
	}

	striper, err := pool.CreateStriper()
	if err != nil {
		err = errors.New("bad ioctx")
		return
	}

	radosReader := &RadosDownloader{
		striper:   striper,
		oid:       oid,
		offset:    startOffset,
		pool:      pool,
		remaining: int64(length),
	}

	return radosReader, nil
}

func (cluster *CephCluster) doSmallRemove(poolname string, oid string) error {
	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return errors.New("Bad poolname")
	}
	defer pool.Destroy()
	return pool.Delete(oid)
}

func (cluster *CephCluster) Remove(poolname string, oid string) error {

	if poolname == backend.SMALL_FILE_POOLNAME {
		return cluster.doSmallRemove(poolname, oid)
	}

	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return errors.New("Bad poolname")
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return errors.New("Bad ioctx")
	}
	defer striper.Destroy()
	// if we do not set our custom layout, rados will infer all objects filename from default layout setting,
	// and some sub objects will not be deleted
	setStripeLayout(striper)

	return striper.Delete(oid)
}

func (cluster *CephCluster) ID() string {
	return cluster.Name
}

func (cluster *CephCluster) GetUsage() (usage backend.Usage, err error) {
	stat, err := cluster.Conn.GetClusterStats()
	if err != nil {
		return usage, err
	}
	usage.UsedSpacePercent = int(stat.Kb_used * uint64(100) / stat.Kb)
	return
}
