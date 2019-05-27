package types

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TypesSuite struct{}

var _ = Suite(&TypesSuite{})

func (ts *TypesSuite) SetUpSuite(c *C) {
}

func (ts *TypesSuite) TearDownSuite(c *C) {
}
