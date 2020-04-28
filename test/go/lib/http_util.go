package lib

import (
	"errors"
	"io/ioutil"
	"net/http"
)

func HTTPRequestToGetObject(url string) (status int, val []byte, err error) {
	res, err := http.Get(url)
	if err != nil {
		return 0, nil, errors.New("httpGet err: " + err.Error() + "url: " + url)
	}
	d, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return 0, nil, errors.New("httpGet read body err: " + err.Error() + "url: " + url)
	}
	return res.StatusCode, d, err
}

func HTTPRequestToGetObjectWithReferer(url string, refererUrl string) (status int, val []byte, err error) {
	var client = &http.Client{}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, nil, errors.New("httpNewRequest err: " + err.Error() + "url: " + url)
	}
	request.Header.Add("Referer", refererUrl)

	res, err := client.Do(request)
	if err != nil {
		return 0, nil, errors.New("httpGet err: " + err.Error() + "url: " + url)
	}
	d, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return 0, nil, errors.New("httpGet read body err: " + err.Error() + "url: " + url)
	}
	return res.StatusCode, d, err
}

func HTTPRequestToGetObjectWithSpecialIP(url string, ipAddress string) (status int, val []byte, err error) {
	var client = &http.Client{}
	request, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, nil, errors.New("httpNewRequest err: " + err.Error() + "url: " + url)
	}

	request.Header.Set("X-Real-Ip", ipAddress)

	res, err := client.Do(request)
	if err != nil {
		return 0, nil, errors.New("httpGet err: " + err.Error() + "url: " + url)
	}
	d, err := ioutil.ReadAll(res.Body)
	defer res.Body.Close()
	if err != nil {
		return 0, nil, errors.New("httpGet read body err: " + err.Error() + "url: " + url)
	}
	return res.StatusCode, d, err
}
