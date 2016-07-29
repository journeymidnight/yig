package storage

import (
	 "errors"
	 "git.letv.cn/ceph/radoshttpd/rados"
	 "io"
	 "container/list"
	 "log"
	 "sync"
	 "fmt"
	 "os"
)


const (
	CEPH_CONFIG_PATH = "./conf/ceph.conf"
	MONTIMEOUT       = "10"
	OSDTIMEOUT       = "10"
	STRIPE_UNIT      = 512 << 10 /*512K*/
	STRIPE_COUNT     = 4
	OBJECT_SIZE      = 4 << 20 /*4M*/
	BUFFERSIZE       = 1 << 20 /* 1M */
	MAX_CHUNK_SIZE   = 4 * BUFFERSIZE /* 4M */
	POOLNAME_RABBIT  = "rabbit"
	POOLNAME_TIGER   = "tiger"
	AIOCONCURRENT    = 4
)

type CephStorage struct {
	Name string
	Conn *rados.Conn
	InstanceId uint64;
	Logger      *log.Logger
	CountMutex  *sync.Mutex
	Counter     uint64
}

/* helper functions */


func NewCephStorage(configFile string, Logger *log.Logger) (*CephStorage) {

	Logger.Printf("Loading Ceph file %s\n", configFile)

	Rados, err := rados.NewConn("admin")
	Rados.SetConfigOption("rados_mon_op_timeout", MONTIMEOUT)
	Rados.SetConfigOption("rados_osd_op_timeout", OSDTIMEOUT)

	err = Rados.ReadConfigFile(configFile)
	if err != nil {
		panic("failed to open ceph.conf")
	}

	err = Rados.Connect()
	if err != nil {
		panic("failed to connect to remote cluster")
	}
	defer Rados.Shutdown()

	name, err := Rados.GetFSID()
	if err != nil {
		panic("failed to get fsid")
	}

	id := Rados.GetInstanceID()

	cluster := CephStorage{Conn:Rados, Name:name, InstanceId:id}

	Logger.Printf("Ceph Cluster %s is ready, InstanceId is %d\n", name , id)
	return &cluster

}


func setStripeLayout(p * rados.StriperPool) int{
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


func pending_has_completed(p *list.List) bool{
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

func wait_pending_front(p * list.List) int{
	/* remove AioCompletion from list */
	e := p.Front()
	p.Remove(e)
	c := e.Value.(*rados.AioCompletion)
	c.WaitForComplete()
	ret := c.GetReturnValue()
	c.Release()
	return ret
}

func drain_pending(p * list.List) int {
	var ret int
	for p.Len() > 0 {
		ret = wait_pending_front(p)
	}
	return ret
}


func (cluster *CephStorage) GetUniqUploadName() string{
	cluster.CountMutex.Lock()
	defer cluster.CountMutex.Unlock()
	cluster.Counter += 1
	oid := fmt.Sprintf("%d:%d", cluster.InstanceId, cluster.Counter)
	return oid
}

func (cluster *CephStorage) put(poolname string, oid string, data io.Reader) error{
	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return errors.New("bad poolname")
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return errors.New("bad ioctx")
	}
	defer striper.Destroy()

	setStripeLayout(&striper)

	buf := make([]byte, BUFFERSIZE)
	/* if the data len in pending_data is bigger than MAX_CHUNK_SIZE, I will flush the data to ceph */
	var pending_data []byte
	var c  *rados.AioCompletion
	pending := list.New()


	var offset uint64 = 0

	for {
		count, err := data.Read(buf)
		if count == 0 {
			break
		}
		if err != nil && err != io.EOF {
				drain_pending(pending)
				return errors.New("read from client failed")
		}

		pending_data = append(pending_data, buf[:count]...)

		if len(pending_data) < MAX_CHUNK_SIZE {
			continue
		}

		/* will write bl to ceph */
		var bl []byte = pending_data[:MAX_CHUNK_SIZE]
		/* now pending_data point to remaining data */
		pending_data = pending_data[MAX_CHUNK_SIZE:]


		c = new(rados.AioCompletion)
		c.Create()
		_, err = striper.WriteAIO(c, oid, bl, uint64(offset))
		if err != nil {
			c.Release()
			drain_pending(pending)
			return errors.New("bad io")
		}
		pending.PushBack(c)

		for pending_has_completed(pending) {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return errors.New("bad io")
			}
		}

		if pending.Len() > AIOCONCURRENT {
			if ret := wait_pending_front(pending); ret < 0 {
				drain_pending(pending)
				return errors.New("bad io")
			}
		}
		offset += uint64(len(bl))
	}

	//write all remaining data
	if len(pending_data) > 0 {
		c = new(rados.AioCompletion)
		c.Create()
		striper.WriteAIO(c, oid, pending_data, uint64(offset))
		pending.PushBack(c)
	}


	//drain_pending
	if ret := drain_pending(pending); ret < 0 {
		return errors.New("bad io")
	}
	return nil
}


type RadosDownloader struct {
    striper       *rados.StriperPool
    oid           string
    offset        int64
}

func (rd *RadosDownloader) Read(p []byte) (n int, err error) {
    count, err := rd.striper.Read(rd.oid, p, uint64(rd.offset))
    if count == 0 {
        return 0, io.EOF
    }
    rd.offset += int64(count)
    return count, err
}

func (rd *RadosDownloader) Seek(offset int64, whence int) (int64, error) {
    return rd.Seek(offset, whence)
}


func (cluster *CephStorage) get(poolname string, oid string, startOffset int64, length int64, writer io.Writer) error{

	pool, err := cluster.Conn.OpenPool(poolname)
	if err != nil {
		return errors.New("bad poolname")
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		return errors.New("bad ioctx")
	}
	defer striper.Destroy()


	radosReader := &RadosDownloader{&striper, oid, startOffset}
	radosReader.Seek(startOffset, os.SEEK_SET)

	limitedReader := &io.LimitedReader{radosReader, length}
	buf := make([]byte, MAX_CHUNK_SIZE)
	io.CopyBuffer(writer, limitedReader, buf)
	return nil
}
