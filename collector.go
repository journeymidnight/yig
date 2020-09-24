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
	PidUsagePrefix           = "u_p_" // User usage redis key prefix ,eg. u_p_hehehehe
	BucketUsagePrefix        = "u_b_" // Bucket usage redis ket prefix ,eg u_b_test
	BucketRequestCountPrefix = "r_b_" // Bucket request count key prefix, e.g. r_b_xxx
	BucketTrafficPrefix      = "t_b_" // Bucket traffic key prefix, e.g. t_b_xxx
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

type KvData struct {
	value int64
	key   string
}

type ResponseCountData struct {
	value  int64
	owner  string
	bucket string
	method string
}

type TrafficData struct {
	value  int64
	owner  string
	bucket string
	method string
}

func newGlobalMetric(namespace string, metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(namespace+"_"+metricName, docString, labels, nil)
}

func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		metrics: map[string]*prometheus.Desc{
			"bucket_usage_byte_metric": newGlobalMetric(namespace,
				"bucket_usage_byte_metric",
				"total bytes used by bucket",
				[]string{"bucket_name", "owner", "storage_class"}),
			"user_usage_byte_metric": newGlobalMetric(namespace,
				"user_usage_byte_metric",
				"total bytes used by user",
				[]string{"owner_id", "storage_class"}),
			"http_response_count_total": newGlobalMetric(namespace,
				"http_response_count_total",
				"total requests served",
				[]string{"bucket_owner", "bucket_name", "method"}),
			"http_response_size_bytes": newGlobalMetric(namespace,
				"http_response_size_bytes",
				"total bytes transferred",
				[]string{"bucket_owner", "bucket_name", "method"}),
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
			ch <- prometheus.MustNewConstMetric(c.metrics["bucket_usage_byte_metric"],
				prometheus.GaugeValue, float64(v.value), bucket, v.owner, v.storageClass)
		}
	}

	GaugeMetricDataForUid := c.GenerateUserUsageData()
	for uid, data := range GaugeMetricDataForUid {
		for _, v := range data {
			ch <- prometheus.MustNewConstMetric(c.metrics["user_usage_byte_metric"],
				prometheus.GaugeValue, float64(v.value), uid, v.key)
		}
	}

	httpCounts := c.GenerateResponseCountData()
	for _, data := range httpCounts {
		for _, v := range data {
			ch <- prometheus.MustNewConstMetric(c.metrics["http_response_count_total"],
				prometheus.GaugeValue, float64(v.value), v.owner, v.bucket, v.method)
		}
	}

	traffics := c.GenerateTrafficData()
	for _, data := range traffics {
		ch <- prometheus.MustNewConstMetric(c.metrics["http_response_size_bytes"],
			prometheus.GaugeValue, float64(data.value), data.owner, data.bucket, data.method)
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
		usageCache, err := redis.RedisConn.GetUsage(key)
		if err != nil {
			helper.Logger.Error("Get usage data from redis for prometheus failed:",
				err.Error())
			return
		}
		datas, err := parseKvString(usageCache)
		if err != nil {
			helper.Logger.Error("Parse usage data from redis for prometheus failed:",
				err.Error())
			return
		}
		for _, data := range datas {
			GaugeMetricData[bucket.Name] = append(GaugeMetricData[bucket.Name], UsageDataWithBucket{data.value, bucket.OwnerId, data.key})
		}
	}
	return
}

// Get bucket usage cache which like <key><value> = <u_p_hehehehe><STANDARD 233333>
func (c *Metrics) GenerateUserUsageData() (GaugeMetricData map[string][]KvData) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		helper.Logger.Error("Get usage data for prometheus failed:",
			err.Error())
		return
	}
	GaugeMetricData = make(map[string][]KvData)
	for _, bucket := range buckets {
		if len(GaugeMetricData[bucket.OwnerId]) == 0 {
			key := PidUsagePrefix + bucket.OwnerId
			usageCache, err := redis.RedisConn.GetUsage(key)
			if err != nil {
				helper.Logger.Error("Get usage data from redis for prometheus failed:",
					err.Error())
				return
			}
			datas, err := parseKvString(usageCache)
			if err != nil {
				helper.Logger.Error("Parse usage data from redis for prometheus failed:",
					err.Error())
				return
			}
			for _, data := range datas {
				GaugeMetricData[bucket.OwnerId] = append(GaugeMetricData[bucket.OwnerId], KvData{value: data.value, key: data.key})
			}
		}
	}
	return
}

func (c *Metrics) GenerateResponseCountData() (responseCountData map[string][]ResponseCountData) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		helper.Logger.Error("Get usage data for prometheus failed:",
			err.Error())
		return
	}
	responseCountData = make(map[string][]ResponseCountData)
	for _, bucket := range buckets {
		key := BucketRequestCountPrefix + bucket.Name
		requestCountString, err := redis.RedisConn.GetUsage(key)
		if err != nil {
			helper.Logger.Error("Get request count from redis for prometheus failed:",
				err.Error())
			return
		}
		data, err := parseKvString(requestCountString)
		if err != nil {
			helper.Logger.Error("Parse request count from redis for prometheus failed:",
				err.Error())
			return
		}
		for _, d := range data {
			responseCountData[bucket.Name] = append(responseCountData[bucket.Name],
				ResponseCountData{
					value:  d.value,
					owner:  bucket.OwnerId,
					bucket: bucket.Name,
					method: d.key,
				})
		}
	}
	return responseCountData
}

func (c *Metrics) GenerateTrafficData() (trafficData map[string]TrafficData) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		helper.Logger.Error("Get usage data for prometheus failed:",
			err.Error())
		return
	}
	trafficData = make(map[string]TrafficData)
	for _, bucket := range buckets {
		key := BucketTrafficPrefix + bucket.Name
		trafficString, err := redis.RedisConn.GetUsage(key)
		if err != nil {
			helper.Logger.Error("Get traffic data from redis for prometheus failed:",
				err.Error())
			return
		}
		traffic, err := strconv.ParseInt(trafficString, 10, 64)
		if err != nil {
			helper.Logger.Error("traffic for", bucket.Name, "not a number:", err)
			return
		}
		trafficData[bucket.Name] = TrafficData{
			value:  traffic,
			owner:  bucket.OwnerId,
			bucket: bucket.Name,
			method: "GET",
		}
	}
	return trafficData
}

//  get usage from redis
//  <Storage-Class1>:<usagenumber>,<Storage-Class2>:<usagenumber>
//  eg. STANDARD:2222
func parseKvString(value string) (datas []*KvData, err error) {
	if value == "" {
		return
	}
	storageClass := strings.Split(value, ",")
	for _, v := range storageClass {
		data := new(KvData)
		allParams := strings.Split(v, ":")
		data.value, err = strconv.ParseInt(allParams[1], 10, 64)
		if err != nil {
			return
		}
		data.key = allParams[0]
		datas = append(datas, data)
	}
	return
}
