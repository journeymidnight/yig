package _go

import (
	"github.com/juju/errors"
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
