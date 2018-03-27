package circuitbreak

import (
	//	"encoding/json"
	"errors"
	"fmt"
	//	"io/ioutil"
	"net/http"
	"sync"
	"time"
)

/*
var cb *CircuitClient

func init() {
	cb = NewCircuitClient()
}
*/

var (
	CircuitCloseErr   = errors.New("circuit has been closed due to too many failed request")
	ExceedMaxRetryErr = errors.New("circuit is in halfopen status and the retry time has exceed threshold")
)

const (
	DefaultStatus    = "open"
	DefaultThreshold = 10
	DefaultMaxRetry  = 3
	DefaultInterval  = 30
)

type CircuitClient struct {
	HttpClient http.Client
	UrlMap     map[string]*UrlItem
}

type UrlItem struct {
	Status       string
	FailNum      int
	Threshold    int
	RetryTime    int
	RetryChan    chan int
	MaxRetryTime int
	Interval     time.Duration
	L            *sync.Mutex
}

func NewCircuitClient() *CircuitClient {
	c := &CircuitClient{UrlMap: make(map[string]*UrlItem, 0)}
	return c
}

func NewUrlItem() *UrlItem {
	urlItem := &UrlItem{
		Status:       DefaultStatus,
		Threshold:    DefaultThreshold,
		MaxRetryTime: DefaultMaxRetry,
		Interval:     time.Duration(DefaultInterval) * time.Second,
		L:            new(sync.Mutex),
	}
	urlItem.RetryChan = make(chan int, urlItem.MaxRetryTime)
	return urlItem
}

func (i *UrlItem) Add() {
	i.L.Lock()
	i.FailNum += 1
	if i.FailNum >= i.Threshold && i.Status != "close" {
		i.Status = "close"
	}
	i.L.Unlock()
}

func (i *UrlItem) Sub() {
	i.L.Lock()
	i.FailNum -= 1
	i.L.Unlock()
}

func (i *UrlItem) SetOpen() {
	i.RetryTime = 0
	i.FailNum = 0
	i.Status = "open"
}
func (i *UrlItem) SetClose() {
	i.RetryTime = 0
	i.Status = "close"
}

func (i *UrlItem) SetHalfOpen() {
	i.RetryTime = 0
	i.FailNum = i.MaxRetryTime
	i.Status = "halfopen"
}

func (cli *CircuitClient) Do(req *http.Request) (res *http.Response, err error) {
	dest := parseUrl(req)
	if _, ok := cli.UrlMap[dest]; !ok {
		cli.UrlMap[dest] = NewUrlItem()
		go checkStatus(cli.UrlMap[dest])
		go exceedRetryHandle(cli.UrlMap[dest])
	}
	item := cli.UrlMap[dest]
	if item.Status == "close" {
		err = CircuitCloseErr
		return
	}
	if item.Status == "halfopen" {
		var L *sync.Mutex = new(sync.Mutex)
		L.Lock()
		item.RetryTime += 1
		L.Unlock()
		if item.RetryTime > item.MaxRetryTime {
			err = ExceedMaxRetryErr
			return
		}
		res, err = cli.HttpClient.Do(req)
		if err == nil {
			item.Sub()
		}
		item.RetryChan <- 1
		return
	}
	res, err = cli.HttpClient.Do(req)
	if err != nil {
		item.Add()
	}
	return
}

func exceedRetryHandle(i *UrlItem) {
	var total, tmp int
	for {
		select {
		case tmp = <-i.RetryChan:
			total += tmp
			if total >= i.MaxRetryTime {
				if i.FailNum == 0 {
					i.SetOpen()
				} else {
					total = 0
					i.SetClose()
				}
			}
		}
	}
}

func checkStatus(i *UrlItem) {
	var ticker = time.NewTicker(i.Interval)
	for {
		select {
		case <-ticker.C:
			if i.Status == "close" {
				i.SetHalfOpen()
			}
		}
	}
}

func parseUrl(req *http.Request) string {
	url := req.URL
	var scheme string
	scheme = url.Scheme
	/*
		port = url.Port()
		if port == "" && url.Scheme == "http" {
			port = "80"
		}
		if port == "" && url.Scheme == "https" {
			port = "443"
		}
	*/
	dest := fmt.Sprintf("%s://%s", scheme, url.Host)
	return dest
}

/*
func main() {
	c := NewCircuitClient()
	//	c.StartLookUp()
	var i int
	for {
		i += 1
		time.Sleep(2 * time.Second)
		fmt.Println("#####################", i)
		request, err := http.NewRequest("GET", "http://localhost/safd", nil)
		if err != nil {
			fmt.Println(err)
		}
		_, err = c.Do(request)
		fmt.Println(*c.UrlMap["http://localhost:80"])
		if err != nil {
			fmt.Println(err)
			continue
		}
				if err == nil {
					body, err := ioutil.ReadAll(res.Body)
					fmt.Println(string(body), err)
				}
		fmt.Println("\n")
	}
}
*/
