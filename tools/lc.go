package main

import (
	"fmt"
	"github.com/journeymidnight/yig/api/datatype"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/iam"
	"github.com/journeymidnight/yig/log"
	"github.com/journeymidnight/yig/meta"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/storage"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	logger      *log.Logger
	yig         *storage.YigStorage
	taskQ       chan types.LifeCycle
	signalQueue chan os.Signal
	waitgroup   sync.WaitGroup
	empty       bool
	stop        bool
)

const (
	SCAN_HBASE_LIMIT = 50
)

func getLifeCycles() {
	var marker string
	logger.Println(5, 5, "all bucket lifecycle handle start")
	waitgroup.Add(1)
	defer waitgroup.Done()
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}

		result, err := yig.MetaStorage.ScanLifeCycle(SCAN_HBASE_LIMIT, marker)
		if err != nil {
			logger.Println(5, "ScanLifeCycle failed", err)
			signalQueue <- syscall.SIGQUIT
			return
		}
		for _, entry := range result.Lcs {
			taskQ <- entry
			marker = entry.BucketName
		}

		if result.Truncated == false {
			empty = true
			return
		}

	}

}

func checkIfExpiration(updateTime time.Time, days int) bool {
	if helper.CONFIG.LcDebug == false {
		return int(time.Since(updateTime).Seconds()) >= days*24*3600
	} else {
		return int(time.Since(updateTime).Seconds()) >= days
	}
}

// If a rule has an empty prifex ,the days in it will be consider as a default days for all objects that not specified in
// other rules. For this reason, we have two conditions to check if a object has expired and should be deleted
//  if defaultConfig == true
//                    for each object           check if object name has a prifix
//  list all objects --------------->loop rules---------------------------------->
//                                                                      |     NO
//                                                                      |--------> days = default days ---
//                                                                      |     YES                         |->delete object if expired
//                                                                      |--------> days = specify days ---
//
//  if defaultConfig == false
//                 for each rule get objects by prefix
//  iterator rules ----------------------------------> loop objects-------->delete object if expired
func retrieveBucket(lc types.LifeCycle) error {
	defaultConfig := false
	defaultDays := 0
	bucket, err := yig.MetaStorage.GetBucket(lc.BucketName, false)
	if err != nil {
		return err
	}
	rules := bucket.LC.Rule
	for _, rule := range rules {
		if rule.Prefix == "" {
			defaultConfig = true
			defaultDays, err = strconv.Atoi(rule.Expiration)
			if err != nil {
				return err
			}
		}
	}
	var request datatype.ListObjectsRequest
	request.Versioned = true
	request.MaxKeys = 1000
	if defaultConfig == true {
		for {
			retObjects, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(bucket.Name, request)
			if err != nil {
				return err
			}

			for _, object := range retObjects {
				prefixMatch := false
				matchDays := 0
				for _, rule := range rules {
					if rule.Prefix == "" {
						continue
					}
					if strings.HasPrefix(object.Name, rule.Prefix) == false {
						continue
					}
					prefixMatch = true
					matchDays, err = strconv.Atoi(rule.Expiration)
					if err != nil {
						return err
					}
				}
				days := 0
				if prefixMatch == true {
					days = matchDays
				} else {
					days = defaultDays
				}
				helper.Debugln("inteval:", time.Since(object.LastModifiedTime).Seconds())
				if checkIfExpiration(object.LastModifiedTime, days) {
					helper.Debugln("come here")
					_, err = yig.DeleteObject(object.BucketName, object.Name, object.VersionId, iam.Credential{})
					if err != nil {
						helper.Logger.Println(5, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
						fmt.Println("[FAILED]", object.BucketName, object.Name, object.VersionId, err)
						continue
					}
					helper.Logger.Println(5, "[DELETED]", object.BucketName, object.Name, object.VersionId)
					fmt.Println("[DELETED]", object.BucketName, object.Name, object.VersionId)
				}
			}
			if truncated == true {
				request.KeyMarker = nextMarker
				request.VersionIdMarker = nextVerIdMarker
			} else {
				break
			}
		}
	} else {
		for _, rule := range rules {
			if rule.Prefix == "" {
				continue
			}
			days, _ := strconv.Atoi(rule.Expiration)
			if err != nil {
				return err
			}
			request.Prefix = rule.Prefix
			for {

				retObjects, _, truncated, nextMarker, nextVerIdMarker, err := yig.ListObjectsInternal(bucket.Name, request)
				if err != nil {
					return err
				}
				for _, object := range retObjects {
					if checkIfExpiration(object.LastModifiedTime, days) {
						_, err = yig.DeleteObject(object.BucketName, object.Name, object.VersionId, iam.Credential{})
						if err != nil {
							logger.Println(5, "failed to delete object:", object.Name, object.BucketName)
							helper.Logger.Println(5, "[FAILED]", object.BucketName, object.Name, object.VersionId, err)
							fmt.Println("[FAILED]", object.BucketName, object.Name, object.VersionId, err)
							continue
						}
						helper.Logger.Println(5, "[DELETED]", object.BucketName, object.Name, object.VersionId)
						fmt.Println("[DELETED]", object.BucketName, object.Name, object.VersionId)
					}
				}
				if truncated == true {
					request.KeyMarker = nextMarker
					request.VersionIdMarker = nextVerIdMarker
				} else {
					break
				}

			}
		}

	}
	return nil
}

func processLifecycle() {
	time.Sleep(time.Second * 1)
	for {
		if stop {
			helper.Logger.Print(5, ".")
			return
		}
		waitgroup.Add(1)
		select {
		case item := <-taskQ:
			err := retrieveBucket(item)
			if err != nil {
				logger.Println(5, "[ERR] Bucket: ", item.BucketName, err)
				fmt.Printf("[ERR] Bucket:%v, %v", item.BucketName, err)
				waitgroup.Done()
				continue
			}
			fmt.Printf("[DONE] Bucket:%s", item.BucketName)
		default:
			if empty == true {
				logger.Println(5, "all bucket lifecycle handle complete. QUIT")
				signalQueue <- syscall.SIGQUIT
				waitgroup.Done()
				return
			}
		}
		waitgroup.Done()
	}
}

func main() {
	helper.SetupConfig()

	f, err := os.OpenFile("lifecycle.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic("Failed to open log file in current dir")
	}
	defer f.Close()
	stop = false
	logger = log.New(f, "[yig]", log.LstdFlags, helper.CONFIG.LogLevel)
	helper.Logger = logger
	yig = storage.New(logger, int(meta.NoCache), false, helper.CONFIG.CephConfigPattern)
	taskQ = make(chan types.LifeCycle, SCAN_HBASE_LIMIT)
	signal.Ignore()
	signalQueue = make(chan os.Signal)

	numOfWorkers := helper.CONFIG.LcThread
	helper.Logger.Println(5, "start lc thread:", numOfWorkers)
	empty = false
	for i := 0; i < numOfWorkers; i++ {
		go processLifecycle()
	}
	go getLifeCycles()
	signal.Notify(signalQueue, syscall.SIGINT, syscall.SIGTERM,
		syscall.SIGQUIT, syscall.SIGHUP)
	for {
		s := <-signalQueue
		switch s {
		case syscall.SIGHUP:
			// reload config file
			helper.SetupConfig()
		default:
			// stop YIG server, order matters
			stop = true
			waitgroup.Wait()
			return
		}
	}

}
