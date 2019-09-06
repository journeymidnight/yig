package api

import (
	"net/http"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/meta/types"
	"github.com/journeymidnight/yig/signature"
)

const RequestContextKey = "RequestContext"

type RequestContext struct {
	RequestId      string
	BucketName     string
	ObjectName     string
	BucketInfo     *types.Bucket
	ObjectInfo     *types.Object
	AuthType       signature.AuthType
	IsBucketDomain bool
}

type Server struct {
	Server *http.Server
}

func (s *Server) Stop() {
	helper.Logger.Print(5, "Stopping API server...")
	helper.Logger.Println(5, "done")
}
