package types

import (
	"github.com/journeymidnight/yig/helper"
	. "gopkg.in/check.v1"
)

type TestData struct {
	Usage int64
	Name  string
}

func (ts *TypesSuite) TestBucketSerialize(c *C) {
	b := &Bucket{
		Name:       "test_bucket",
		Usage:      100,
		FileCounts: 120,
	}

	fields, err := b.Serialize()
	c.Assert(err, Equals, nil)
	c.Assert(fields, Not(Equals), nil)
	c.Assert(len(fields) > 0, Equals, true)
	body, ok := fields[FIELD_NAME_BODY]
	c.Assert(ok, Equals, true)
	c.Assert(body, Not(Equals), nil)

	b2 := &Bucket{}
	str, ok := body.(string)
	c.Assert(ok, Equals, true)
	c.Assert(str != "", Equals, true)
	c.Logf("str: %s", str)
	err = helper.MsgPackUnMarshal([]byte(str), b2)
	c.Assert(err, Equals, nil)
	c.Assert(b2, Not(Equals), nil)
}
