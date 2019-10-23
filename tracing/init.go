package tracing

import (
	"fmt"
	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go/rpcmetrics"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
	"io"
)

func Init(serviceName string, metricsFactory metrics.Factory, logger log.Factory) (opentracing.Tracer, io.Closer, error) {
	cfg, err := config.FromEnv()
	if err != nil {
		logger.Bg().Fatal("cannot parse Jaeger env vars", zap.Error(err))
	}
	cfg.ServiceName = serviceName
	cfg.Sampler.Type = "const"
	cfg.Sampler.Param = 1
	cfg.Reporter.LocalAgentHostPort = "jaeger:6831"

	jaegerLogger := jaegerLoggerAdapter{logger.Bg()}

	metricsFactory = metricsFactory.Namespace(metrics.NSOptions{Name: serviceName, Tags: nil})
	tracer, closer, err := cfg.NewTracer(
		config.Logger(jaegerLogger),
		config.Metrics(metricsFactory),
		config.Observer(rpcmetrics.NewObserver(metricsFactory, rpcmetrics.DefaultNameNormalizer)),
	)
	helper.Logger.Info( cfg.Sampler, cfg.Reporter, err)
	if err != nil {
		//logger.Bg().Fatal("cannot initialize Jaeger Tracer", zap.Error(err))
		helper.Logger.Error("cannot initialize Jaeger Tracer", err)
	}

	return tracer, closer, err
}

type jaegerLoggerAdapter struct {
	logger log.Logger
}

func (l jaegerLoggerAdapter) Error(msg string) {
	l.logger.Error(msg)
}

func (l jaegerLoggerAdapter) Infof(msg string, args ...interface{}) {
	l.logger.Info(fmt.Sprintf(msg, args...))
}
