package ceph_test

import (
	"bytes"
	"github.com/journeymidnight/yig-restore/backend"
	"github.com/journeymidnight/yig-restore/ceph"
	"github.com/journeymidnight/yig-restore/helper"
	"testing"
	"time"
)

func SetupMockCeph() ceph.CephCluster {
	helper.Conf.UploadMinChunkSize = 512 << 10
	helper.Conf.UploadMaxChunkSize = 8 << 20

	striper := MockStriperPool{
		FixedReadOverhead:  3 * time.Millisecond,
		FixedWriteOverhead: 5 * time.Millisecond,
	}
	pool := MockPool{
		MockStriper:        striper,
		FixedReadOverhead:  3 * time.Millisecond,
		FixedWriteOverhead: 5 * time.Millisecond,
	}
	conn := MockRadosConn{
		MockPool: pool,
	}
	cluster := ceph.CephCluster{
		Name:       "benchmark",
		Conn:       conn,
		InstanceId: 0,
	}
	return cluster
}

func BenchmarkCephCluster_Put(b *testing.B) {
	mockData120K := make([]byte, 120<<10)
	for i := 0; i < len(mockData120K); i++ {
		mockData120K[i] = uint8(i)
	}
	mockData10M := make([]byte, 10<<20)
	for i := 0; i < len(mockData10M); i++ {
		mockData10M[i] = uint8(i)
	}
	mockData30M := make([]byte, 30<<20)
	for i := 0; i < len(mockData30M); i++ {
		mockData30M[i] = uint8(i)
	}
	mockData100M := make([]byte, 100<<20)
	for i := 0; i < len(mockData100M); i++ {
		mockData100M[i] = uint8(i)
	}
	cluster := SetupMockCeph()
	b.Run("Put small pool 120K", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(mockData120K)
			oid, size, err := cluster.Put(backend.SMALL_FILE_POOLNAME, reader)
			if err != nil {
				b.Error("Put error:", err)
			}
			b.Log("oid:", oid, "size:", size)
		}
	})
	b.Run("Put big pool 10M", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(mockData10M)
			oid, size, err := cluster.Put(backend.BIG_FILE_POOLNAME, reader)
			if err != nil {
				b.Error("Put error:", err)
			}
			b.Log("oid:", oid, "size:", size)
		}
	})
	b.Run("Put big pool 30M", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(mockData30M)
			oid, size, err := cluster.Put(backend.BIG_FILE_POOLNAME, reader)
			if err != nil {
				b.Error("Put error:", err)
			}
			b.Log("oid:", oid, "size:", size)
		}
	})
	b.Run("Put big pool 100M", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader := bytes.NewReader(mockData100M)
			oid, size, err := cluster.Put(backend.BIG_FILE_POOLNAME, reader)
			if err != nil {
				b.Error("Put error:", err)
			}
			b.Log("oid:", oid, "size:", size)
		}
	})
}
