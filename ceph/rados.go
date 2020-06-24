package ceph

import "github.com/journeymidnight/radoshttpd/rados"

// Interfaces for underlying rados lib, mainly to ease testing/mocking

type RadosConn interface {
	OpenPool(name string) (Pool, error)
	GetClusterStats() (rados.ClusterStat, error)
	Shutdown()
}

type Pool interface {
	Write(oid string, data []byte, offset uint64) error
	Read(oid string, data []byte, offset uint64) (int, error)
	Delete(oid string) error
	Truncate(oid string, size uint64) error
	Destroy()
	CreateStriper() (StriperPool, error)
	WriteSmallObject(oid string, data []byte) error
}

type StriperPool interface {
	Read(oid string, data []byte, offset uint64) (int, error)
	Write(oid string, data []byte, offset uint64) (int, error)
	WriteAIO(oid string, data []byte, offset uint64) (AioCompletion, error)
	Delete(oid string) error
	Destroy()
	SetLayoutStripeUnit(unit uint) int
	SetLayoutStripeCount(count uint) int
	SetLayoutObjectSize(size uint) int
}

type AioCompletion interface {
	WaitForComplete()
	Release()
	IsComplete() int
	GetReturnValue() int
}

type radosConn struct {
	*rados.Conn
}

func (c radosConn) OpenPool(name string) (Pool, error) {
	p, err := c.Conn.OpenPool(name)
	if err != nil {
		return nil, err
	}
	return pool{p}, nil
}

type pool struct {
	*rados.Pool
}

func (p pool) CreateStriper() (StriperPool, error) {
	s, err := p.Pool.CreateStriper()
	if err != nil {
		return nil, err
	}
	return striperPool{&s}, nil
}

type striperPool struct {
	*rados.StriperPool
}

func (sp striperPool) WriteAIO(oid string,
	data []byte, offset uint64) (AioCompletion, error) {

	aio := &rados.AioCompletion{}
	err := aio.Create()
	if err != nil {
		return nil, err
	}
	_, err = sp.StriperPool.WriteAIO(aio, oid, data, offset)
	return aio, err
}
