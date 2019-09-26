package api

import (
	"github.com/journeymidnight/yig/log"
	"net/http"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

type RequestContextKeyType string
const RequestContextKey RequestContextKeyType = "RequestContext"

type RequestContext struct {
	RequestId      string
	BucketName     string
	ObjectName     string
	BucketInfo     *types.Bucket
	ObjectInfo     *types.Object
	AuthType       signature.AuthType
	IsBucketDomain bool
	Logger     log.Logger
}

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Info("Server stopped")
}
