package api

import (
	"git.letv.cn/yig/yig/helper"
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
	helper.Logger.Print("Stopping API server...")
	rateLimiter.ShutdownServer()
	helper.Logger.Println("done")
}
