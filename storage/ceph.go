package storage

import (
	"container/list"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/journeymidnight/radoshttpd/rados"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
)

const (
	MON_TIMEOUT         = "10"
	OSD_TIMEOUT         = "10"
	STRIPE_UNIT         = 512 << 10 /* 512K */
	STRIPE_COUNT        = 1
	OBJECT_SIZE         = 4 << 20         /* 4M */
	BUFFER_SIZE         = 1 << 20         /* 1M */
	MIN_CHUNK_SIZE      = 512 << 10       /* 512K */
	MAX_CHUNK_SIZE      = 4 * BUFFER_SIZE /* 4M */
	SMALL_FILE_POOLNAME = "rabbit"
	BIG_FILE_POOLNAME   = "tiger"
	BIG_FILE_THRESHOLD  = 128 << 10 /* 128K */
	AIO_CONCURRENT      = 4
)

type CephStorage struct {
	Name       string
	Conn       *rados.Conn
	InstanceId uint64
	Logger     *log.Logger
	CountMutex *sync.Mutex
	Counter    uint64
}

func NewCephStorage(configFile string, logger *log.Logger) *CephStorage {

	logger.Printf(5, "Loading Ceph file %s\n", configFile)

	Rados, err := rados.NewConn("admin")
	Rados.SetConfigOption("rados_mon_op_timeout", MON_TIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSD_TIMEOUT)

	err = Rados.ReadConfigFile(configFile)
	if err != nil {
		helper.Logger.Printf(0, "Failed to open ceph.conf: %s\n", configFile)
		return nil
	}

	err = Rados.Connect()
	if err != nil {
		helper.Logger.Printf(0, "Failed to connect to remote cluster: %s\n", configFile)
		return nil
	}

	name, err := Rados.GetFSID()
	if err != nil {
		helper.Logger.Printf(0, "Failed to get FSID: %s\n", configFile)
		Rados.Shutdown()
		return nil
	}

	id := Rados.GetInstanceID()

	cluster := CephStorage{
		Conn:       Rados,
		Name:       name,
		InstanceId: id,
		Logger:     logger,
		CountMutex: new(sync.Mutex),
	}

	logger.Printf(5, "Ceph Cluster %s is ready, InstanceId is %d\n", name, id)
	return &cluster
}

func setStripeLayout(p *rados.StriperPool) int {
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
	c := e.Value.(*rados.AioCompletion)
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
	c := e.Value.(*rados.AioCompletion)
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

func (cluster *CephStorage) GetUniqUploadName() string {
	cluster.CountMutex.Lock()
	defer cluster.CountMutex.Unlock()
	cluster.Counter += 1
	oid := fmt.Sprintf("%d:%d", cluster.InstanceId, cluster.Counter)
	return oid
}

func (c *CephStorage) Shutdown() {
	c.Conn.Shutdown()
}

func (cluster *CephStorage) doSmallPut(poolname string, oid string, data io.Reader) (size int64, err error) {
	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return 0, errors.New("Bad poolname")
	}
	defer pool.Destroy()

	buf, err := ioutil.ReadAll(data)
	size = int64(len(buf))
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
	pool      *rados.Pool
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

func (cluster *CephStorage) Put(poolname string, oid string, data io.Reader) (size int64, err error) {

	if poolname == SMALL_FILE_POOLNAME {
		return cluster.doSmallPut(poolname, oid, data)
	}

	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return 0, errors.New("Bad poolname")
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return 0, errors.New("Bad ioctx")
	}
	defer striper.Destroy()

	setStripeLayout(&striper)

	/* if the data len in pending_data is bigger than current_upload_window, I will flush the data to ceph */
	/* current_upload_window could not dynamically increase or shrink */

	var c *rados.AioCompletion
	pending := list.New()
	var current_upload_window = MIN_CHUNK_SIZE /* initial window size as MIN_CHUNK_SIZE, max size is MAX_CHUNK_SIZE */
	var pending_data = make([]byte, current_upload_window)

	var slice_offset = 0
	var slice = pending_data[0:current_upload_window]

	var offset uint64 = 0

	for {

		count, err := data.Read(slice)
		if count == 0 {
			break
		}

		slice_offset += count
		slice = pending_data[slice_offset:current_upload_window]
		if err != nil && err != io.EOF {
			drain_pending(pending)
			return 0, errors.New("Read from client failed")
		}

		//is pending_data full?
		if slice_offset < len(pending_data) {
			continue
		}

		/* pending data is full now */
		var bl []byte = pending_data[:]

		/* allocate a new pending data */
		pending_data = make([]byte, current_upload_window)
		slice_offset = 0
		slice = pending_data[0:current_upload_window]

		c = new(rados.AioCompletion)
		c.Create()
		_, err = striper.WriteAIO(c, oid, bl, offset)
		if err != nil {
			c.Release()
			drain_pending(pending)
			return 0, errors.New("Bad io")
		}
		pending.PushBack(c)

		for pending_has_completed(pending) {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return 0, errors.New("Error drain_pending in pending_has_completed")
			}
		}

		if pending.Len() > AIO_CONCURRENT {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return 0, errors.New("Error wait_pending_front")
			}
		}
		offset += uint64(len(bl))
	}

	size = int64(uint64(slice_offset) + offset)
	//write all remaining data
	if slice_offset > 0 {
		c = new(rados.AioCompletion)
		c.Create()
		striper.WriteAIO(c, oid, pending_data[:slice_offset], offset)
		pending.PushBack(c)
	}

	//drain_pending
	if ret := drain_pending(pending); ret < 0 {
		return 0, errors.New("Error drain_pending")
	}
	return size, nil
}

type RadosDownloader struct {
	striper   *rados.StriperPool
	oid       string
	offset    int64
	remaining int64
	pool      *rados.Pool
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

func (cluster *CephStorage) getReader(poolName string, oid string, startOffset int64,
	length int64) (reader io.ReadCloser, err error) {

	if poolName == SMALL_FILE_POOLNAME {
		pool, e := cluster.Conn.OpenPool(poolName)
		if e != nil {
			err = errors.New("bad poolname")
			return
		}
		radosSmallReader := &RadosSmallDownloader{
			oid:       oid,
			offset:    startOffset,
			pool:      pool,
			remaining: length,
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
		striper:   &striper,
		oid:       oid,
		offset:    startOffset,
		pool:      pool,
		remaining: length,
	}

	return radosReader, nil
}

// Works together with `wrapAlignedEncryptionReader`, see comments there.
func (cluster *CephStorage) getAlignedReader(poolName string, oid string, startOffset int64,
	length int64) (reader io.ReadCloser, err error) {

	alignedOffset := startOffset / AES_BLOCK_SIZE * AES_BLOCK_SIZE
	length += startOffset - alignedOffset
	return cluster.getReader(poolName, oid, alignedOffset, length)
}

/*
func (cluster *CephStorage) get(poolName string, oid string, startOffset int64,
	length int64, writer io.Writer) error {

	reader, err := cluster.getReader(poolName, oid, startOffset, length)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := make([]byte, MAX_CHUNK_SIZE)
	_, err = io.CopyBuffer(writer, reader, buf)
	return err
}
*/

func (cluster *CephStorage) doSmallRemove(poolname string, oid string) error {
	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return errors.New("Bad poolname")
	}
	defer pool.Destroy()
	return pool.Delete(oid)
}

func (cluster *CephStorage) Remove(poolname string, oid string) error {

	if poolname == SMALL_FILE_POOLNAME {
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

	return striper.Delete(oid)
}

func (cluster *CephStorage) GetUsedSpacePercent() (pct int, err error) {
	stat, err := cluster.Conn.GetClusterStats()
	if err != nil {
		return 0, errors.New("Stat error")
	}
	pct = int(stat.Kb_used * uint64(100) / stat.Kb)
	return
}
