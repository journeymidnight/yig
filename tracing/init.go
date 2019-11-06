package tracing

import (
	"github.com/journeymidnight/yig/helper"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	"io"
)

func Init(serviceName string) (opentracing.Tracer, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		helper.Logger.Info("cannot parse Jaeger env vars", err)
	}
	cfg.ServiceName = serviceName
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	cfg.Reporter.LocalAgentHostPort = "jaeger:6831"

	tracer, closer, err := cfg.New(serviceName, config.Logger(jaeger.StdLogger))
	if err != nil {
		helper.Logger.Info("ERROR: cannot init Jaeger: ", err)
		helper.CONFIG.OpentracingSwitch = false
	}
	return tracer, closer, err
}
