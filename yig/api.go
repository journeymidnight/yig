package main

import (
	"github.com/codegangsta/martini"
	"git.letv.cn/yig/yig/rados"
	"fmt"
	"net/http"
	"encoding/xml"
)

type ErrorResponse struct {
	XMLName	xml.Name `xml:"Error"`
	StatusCode int `xml:"-"`
	Code	string   `xml:"Code"`
	Message string   `xml:"Message"`
	Resource string  `xml:"Resource,omitempty"`
	RequestId string `xml:"RequestId,omitempty"` // TODO: ?
}

func responseWithError(w http.ResponseWriter, e *ErrorResponse)  {
	out, err := xml.Marshal(e)
	if err != nil {
		logger.Println("Failed to marshal XML: ", e)
		return
	}
	w.WriteHeader(e.StatusCode)
	w.Write(xml.Header + out)
}

func InfoHandler(params martini.Params, w http.ResponseWriter, r *http.Request, conn *rados.Conn) {
	poolname := params["pool"]
	soid := params["soid"]
	pool, err := conn.OpenPool(poolname)
	if err != nil {
		logger.Println("URL:", r.URL, "open pool failed")
		responseWithError(w, &ErrorResponse{
				StatusCode: http.StatusNotFound,
				Code: "InvalidPool",
				Message: "Failed to open pool",
			})
		return
	}
	defer pool.Destroy()

	striper, err := pool.CreateStriper()
	if err != nil {
		logger.Println("URL:", r.URL, "Create Striper failed")
		responseWithError(w, &ErrorResponse{
				StatusCode:http.StatusNotFound,
				Code:"InvalidPool",
				Message:"Failed to create striper",
			})
		return
	}
	defer striper.Destroy()

	size, _, err := striper.State(soid)
	if err != nil {
		logger.Println("URL:%s, failed to get object "+soid, r.URL)
		responseWithError(w, &ErrorResponse{
				StatusCode:http.StatusNotFound,
				Code:"InvalidPool",
				Message:"Failed to get object",
			})
		return
	}
	/* use json format */
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(fmt.Sprintf("{\"size\":%d}", size)))
	return
}

func CephStatusHandler(params martini.Params, w http.ResponseWriter, r *http.Request, conn *rados.Conn) {
	c, err := conn.Status()
	if err != nil {
		responseWithError(w, &ErrorResponse{
				StatusCode:http.StatusInternalServerError,
				Code:"InternalError",
			})
		return
	}
	w.Write([]byte(c))
}


func setupHandlers(m *martini.ClassicMartini)  {
	m.Get("/threads", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, fmt.Sprintf("%d\n", concurrentRequestNumber.size()))
	})
	m.Get("/cephstatus", RequestLimit(), CephStatusHandler)
	m.Get("/info/(?P<pool>[A-Za-z0-9]+)/(?P<soid>[^/]+)", RequestLimit(), InfoHandler)
}