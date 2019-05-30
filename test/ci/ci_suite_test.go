package ci

import (
	"os"
	"testing"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/redis"
	"github.com/journeymidnight/yig/storage"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type CISuite struct {
	// this YigStorage is readonly.
	// we create it just for verify data.
	storage *storage.YigStorage
	logf    *os.File
}

var _ = Suite(&CISuite{})

var logger *log.Logger

func (cs *CISuite) SetUpSuite(c *C) {
	var err error
	helper.SetupConfig()
	cs.logf, err = os.OpenFile("./yig_test.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		c.Fatalf("cannot create yig_test.log, err: %v", err)
	}

	logger := log.New(cs.logf, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	if helper.CONFIG.MetaCacheType > 0 || helper.CONFIG.EnableDataCache {
		redis.Initialize()
	}

	cs.storage = storage.New(logger, helper.CONFIG.MetaCacheType, helper.CONFIG.EnableDataCache, helper.CONFIG.CephConfigPattern)
}

func (cs *CISuite) TearDownSuite(c *C) {
	cs.storage.Stop()
	cs.logf.Close()
}
