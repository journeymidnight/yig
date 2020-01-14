package main

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/redis"
	"github.com/prometheus/client_golang/prometheus"
	"strconv"
	"strings"
	"sync"
)

const (
	PidUsagePrefix    = "u_p_" // User usage redis key prefix ,eg. u_p_hehehehe
	BucketUsagePrefix = "u_b_" // Bucket usage redis ket prefix ,eg u_b_test
)

type Metrics struct {
	metrics map[string]*prometheus.Desc
	mutex   sync.Mutex
}

type UsageDataWithBucket struct {
	value        int64
	owner        string
	storageClass string
}

type UsageData struct {
	value        int64
	storageClass string
}

func newGlobalMetric(namespace string, metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(namespace+"_"+metricName, docString, labels, nil)
}

func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		metrics: map[string]*prometheus.Desc{
			"bucket_usage_byte_metric": newGlobalMetric(namespace, "bucket_usage_byte_metric", "The description of bucket_usage_byte_metric", []string{"bucket_name", "owner", "storage_class"}),
			"user_usage_byte_metric":   newGlobalMetric(namespace, "user_usage_byte_metric", "The description of User_usage_byte_metric", []string{"owner_id", "storage_class"}),
		},
	}
}

func (c *Metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m
	}
}

func (c *Metrics) Collect(ch chan<- prometheus.Metric) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	GaugeMetricDataForBucket := c.GenerateBucketUsageData()
	for bucket, data := range GaugeMetricDataForBucket {
		for _, v := range data {
			ch <- prometheus.MustNewConstMetric(c.metrics["bucket_usage_byte_metric"], prometheus.GaugeValue, float64(v.value), bucket, v.owner, v.storageClass)
		}
	}

	GaugeMetricDataForUid := c.GenerateUserUsageData()
	for uid, data := range GaugeMetricDataForUid {
		for _, v := range data {
			ch <- prometheus.MustNewConstMetric(c.metrics["user_usage_byte_metric"], prometheus.GaugeValue, float64(v.value), uid, v.storageClass)
		}
	}
}

// Get bucket usage cache which like <key><value> = <u_b_test><STANDARD:233333>
func (c *Metrics) GenerateBucketUsageData() (GaugeMetricData map[string][]UsageDataWithBucket) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		helper.Logger.Error("Get usage data for prometheus failed:",
			err.Error())
		return
	}
	GaugeMetricData = make(map[string][]UsageDataWithBucket)
	for _, bucket := range buckets {
		key := BucketUsagePrefix + bucket.Name
		usageCache, err := redis.GetUsage(key)
		if err != nil {
			helper.Logger.Error("Get usage data from redis for prometheus failed:",
				err.Error())
			return
		}
		datas, err := parseUsage(usageCache)
		if err != nil {
			helper.Logger.Error("Parse usage data from redis for prometheus failed:",
				err.Error())
			return
		}
		for _, data := range datas {
			if len(GaugeMetricData[bucket.Name]) == 0 {
				GaugeMetricData[bucket.Name] = []UsageDataWithBucket{{data.value, bucket.OwnerId, data.storageClass}}
			} else {
				GaugeMetricData[bucket.Name] = append(GaugeMetricData[bucket.Name], UsageDataWithBucket{data.value, bucket.OwnerId, data.storageClass})
			}
		}
	}
	return
}

// Get bucket usage cache which like <key><value> = <u_p_hehehehe><STANDARD 233333>
func (c *Metrics) GenerateUserUsageData() (GaugeMetricData map[string][]UsageData) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		helper.Logger.Error("Get usage data for prometheus failed:",
			err.Error())
		return
	}
	GaugeMetricData = make(map[string][]UsageData)
	for _, bucket := range buckets {
		if len(GaugeMetricData[bucket.OwnerId]) == 0 {
			key := PidUsagePrefix + bucket.OwnerId
			usageCache, err := redis.GetUsage(key)
			if err != nil {
				helper.Logger.Error("Get usage data from redis for prometheus failed:",
					err.Error())
				return
			}
			datas, err := parseUsage(usageCache)
			if err != nil {
				helper.Logger.Error("Parse usage data from redis for prometheus failed:",
					err.Error())
				return
			}
			for _, data := range datas {
				if len(GaugeMetricData[bucket.OwnerId]) == 0 {
					GaugeMetricData[bucket.OwnerId] = []UsageData{{value: data.value, storageClass: data.storageClass}}
				} else {
					GaugeMetricData[bucket.OwnerId] = append(GaugeMetricData[bucket.OwnerId], UsageData{value: data.value, storageClass: data.storageClass})
				}
			}
		}
	}
	return
}

//  get usage from redis
//  <Storage-Class1>:<usagenumber>,<Storage-Class2>:<usagenumber>
//  eg. STANDARD:2222
func parseUsage(value string) (datas []*UsageData, err error) {
	if value == "" {
		return
	}
	storageClass := strings.Split(value, ",")
	for _, v := range storageClass {
		data := new(UsageData)
		allParams := strings.Split(v, ":")
		data.value, err = strconv.ParseInt(allParams[1], 10, 64)
		if err != nil {
			return
		}
		data.storageClass = allParams[0]
		datas = append(datas, data)
	}
	return
}
