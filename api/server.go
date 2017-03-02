package api

import (
	"legitlab.letv.cn/yig/yig/helper"
	"net/http"
)

type ContextKey int

const (
	RequestId ContextKey = iota
)

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Print(5, "Stopping API server...")
	helper.Logger.Println(5, "done")
}
