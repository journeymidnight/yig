package ci

import (
	"fmt"

	. "github.com/journeymidnight/yig/test/go/lib"
	. "gopkg.in/check.v1"
)

func (cs *CISuite) TestBasicBucketUsage(c *C) {
	bn := "buckettest"
	key := "objt1"
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	b, err := cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage == 0, Equals, true)
	randUtil := &RandUtil{}
	val := randUtil.RandString(128 << 10)
	err = sc.PutObject(bn, key, val)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	// check the bucket usage.
	b, err = cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(128<<10))
}

func (cs *CISuite) TestManyObjectsForBucketUsage(c *C) {
	bn := "buckettest"
	count := 100
	size := (128 << 10)
	totalObjSize := int64(0)
	var objNames []string
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	b, err := cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage == 0, Equals, true)
	for i := 0; i < count; i++ {
		randUtil := &RandUtil{}
		val := randUtil.RandString(size)
		key := fmt.Sprintf("objt%d", i+1)
		err = sc.PutObject(bn, key, val)
		c.Assert(err, Equals, nil)
		totalObjSize = totalObjSize + int64(size)
		objNames = append(objNames, key)
	}

	defer func() {
		for _, obj := range objNames {
			err = sc.DeleteObject(bn, obj)
			c.Assert(err, Equals, nil)
			totalObjSize = totalObjSize - int64(size)
			b, err = cs.storage.GetBucket(bn)
			c.Assert(err, Equals, nil)
			c.Assert(b, Not(Equals), nil)
			c.Assert(b.Usage, Equals, totalObjSize)
		}
	}()
	// check the bucket usage.
	b, err = cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalObjSize)
}

func (cs *CISuite) TestBucketUsageForAppendObject(c *C) {
	bn := "buckettest"
	key := "objtappend"
	totalSize := int64(0)
	nextSize := int64(0)
	sc := NewS3()
	err := sc.MakeBucket(bn)
	c.Assert(err, Equals, nil)
	b, err := cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	defer sc.DeleteBucket(bn)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, int64(0))
	ru := &RandUtil{}
	size := (128 << 10)
	body := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, body, 0)
	c.Assert(err, Equals, nil)
	defer sc.DeleteObject(bn, key)
	totalSize = totalSize + int64(size)
	b, err = cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalSize)
	appendBody := ru.RandString(size)
	nextSize, err = sc.AppendObject(bn, key, appendBody, int64(size))
	c.Assert(err, Equals, nil)
	totalSize = totalSize + int64(size)
	c.Assert(nextSize, Equals, totalSize)
	b, err = cs.storage.GetBucket(bn)
	c.Assert(err, Equals, nil)
	c.Assert(b, Not(Equals), nil)
	c.Assert(b.Usage, Equals, totalSize)
}
