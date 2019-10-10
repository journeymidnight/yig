package ceph_test

import (
	"github.com/journeymidnight/radoshttpd/rados"
	"github.com/journeymidnight/yig/ceph"
	"time"
)

// Mocked rados connection for testing

// Simulate total processing time as:
// fixed processing time + processing time relative to data size

type MockRadosConn struct {
	MockPool MockPool
}

func (c MockRadosConn) OpenPool(name string) (ceph.Pool, error) {
	return c.MockPool, nil
}

func (c MockRadosConn) GetClusterStats() (rados.ClusterStat, error) {
	return rados.ClusterStat{}, nil
}

func (c MockRadosConn) Shutdown() {
	return
}

type MockPool struct {
	MockStriper        MockStriperPool
	FixedReadOverhead  time.Duration
	FixedWriteOverhead time.Duration
}

func (p MockPool) Write(oid string, data []byte, offset uint64) error {
	processingTime := p.FixedWriteOverhead + time.Duration(len(data))
	time.Sleep(processingTime)
	return nil
}

func (p MockPool) Read(oid string, data []byte, offset uint64) (int, error) {
	processingTime := p.FixedReadOverhead + time.Duration(len(data))
	time.Sleep(processingTime)
	return len(data), nil
}

func (p MockPool) Delete(oid string) error {
	time.Sleep(p.FixedWriteOverhead)
	return nil
}

func (p MockPool) Truncate(oid string, size uint64) error {
	time.Sleep(p.FixedWriteOverhead)
	return nil
}

func (p MockPool) Destroy() {
	return
}

func (p MockPool) CreateStriper() (ceph.StriperPool, error) {
	return p.MockStriper, nil
}

func (p MockPool) WriteSmallObject(oid string, data []byte) error {
	processingTime := p.FixedWriteOverhead + time.Duration(len(data))
	time.Sleep(processingTime)
	return nil
}

type MockStriperPool struct {
	FixedReadOverhead  time.Duration
	FixedWriteOverhead time.Duration
}

func (sp MockStriperPool) Read(oid string, data []byte, offset uint64) (int, error) {
	processingTime := sp.FixedReadOverhead + time.Duration(len(data))
	time.Sleep(processingTime)
	return len(data), nil
}

func (sp MockStriperPool) Write(oid string, data []byte, offset uint64) (int, error) {
	processingTime := sp.FixedWriteOverhead + time.Duration(len(data))
	time.Sleep(processingTime)
	return len(data), nil
}

func (sp MockStriperPool) WriteAIO(oid string, data []byte, offset uint64) (ceph.AioCompletion, error) {
	return MockAioCompletion{
		CreatedAt:      time.Now(),
		ProcessingTime: sp.FixedWriteOverhead + time.Duration(len(data)),
	}, nil
}

func (sp MockStriperPool) Delete(oid string) error {
	time.Sleep(sp.FixedWriteOverhead)
	return nil
}

func (sp MockStriperPool) Destroy() {
	return
}

func (sp MockStriperPool) SetLayoutStripeUnit(uint uint) int {
	return 0
}

func (sp MockStriperPool) SetLayoutStripeCount(count uint) int {
	return 0
}

func (sp MockStriperPool) SetLayoutObjectSize(size uint) int {
	return 0
}

type MockAioCompletion struct {
	CreatedAt      time.Time
	ProcessingTime time.Duration
}

func (aio MockAioCompletion) WaitForComplete() {
	finishTime := aio.CreatedAt.Add(aio.ProcessingTime)
	if time.Now().After(finishTime) {
		return
	}
	time.Sleep(finishTime.Sub(time.Now()))
}

func (aio MockAioCompletion) Release() {
	return
}

func (aio MockAioCompletion) IsComplete() int {
	finishTime := aio.CreatedAt.Add(aio.ProcessingTime)
	if time.Now().After(finishTime) {
		return 1 // true
	}
	return 0 // false
}

func (aio MockAioCompletion) GetReturnValue() int {
	return 0
}
