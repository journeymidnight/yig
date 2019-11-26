package api

import (
	"net/http"

	"github.com/journeymidnight/yig/helper"
)

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Info("Server stopped")
}
