package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

type Metrics struct {
	metrics map[string]*prometheus.Desc
	mutex   sync.Mutex
}
type UsageData struct {
	value int64
	owner string
}

func newGlobalMetric(namespace string, metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(namespace+"_"+metricName, docString, labels, nil)
}

func NewMetrics(namespace string) *Metrics {
	return &Metrics{
		metrics: map[string]*prometheus.Desc{
			"bucket_usage_byte_metric": newGlobalMetric(namespace, "bucket_usage_byte_metric", "The description of bucket_usage_byte_metric", []string{"bucket_name", "owner"}),
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

	GaugeMetricData := c.GenerateUsageData()
	for bucket, data := range GaugeMetricData {
		ch <- prometheus.MustNewConstMetric(c.metrics["bucket_usage_byte_metric"], prometheus.GaugeValue, float64(data.value), bucket, data.owner)
	}
}

func (c *Metrics) GenerateUsageData() (GaugeMetricData map[string]UsageData) {
	buckets, err := adminServer.Yig.MetaStorage.GetBuckets()
	if err != nil {
		adminServer.Yig.Logger.Println(5, "get usage data for prometheus failed:", err.Error())
		return
	}
	GaugeMetricData = make(map[string]UsageData)
	for _, bucket := range buckets {
		GaugeMetricData[bucket.Name] = UsageData{bucket.Usage, bucket.OwnerId}
	}
	return
}
