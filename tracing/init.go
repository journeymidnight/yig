package tracing

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"io"
)

func Init(serviceName string) (opentracing.Tracer, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		helper.Logger.Info("cannot parse Jaeger env vars", err)
	}
	cfg.Sampler.Type = helper.CONFIG.OpentracingSamplerType
	cfg.Sampler.Param = helper.CONFIG.OpentracingSamplerParam
	cfg.Reporter.LocalAgentHostPort = helper.CONFIG.OpentracingJaegerPort

	tracer, closer, err := cfg.New(serviceName)
	if err != nil {
		helper.Logger.Info("ERROR: cannot init Jaeger: ", err)
		helper.CONFIG.OpentracingEnable = false
	}
	return tracer, closer, err
}
