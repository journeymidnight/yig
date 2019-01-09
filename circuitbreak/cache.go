package circuitbreak

import (
	"errors"
	"github.com/cep21/circuit"
	"github.com/cep21/circuit/closers/hystrix"
	"time"
	"github.com/journeymidnight/yig/helper"
)

var (
	CacheCircuitIsOpenErr = errors.New("cache circuit is open now!")
)

func NewCacheCircuit() *circuit.Circuit {
	return circuit.NewCircuitFromConfig("YigCache", circuit.Config{
		General: circuit.GeneralConfig{
			OpenToClosedFactory: hystrix.CloserFactory(hystrix.ConfigureCloser{
				SleepWindow:                  time.Duration(helper.CONFIG.CacheCircuitCloseSleepWindow) * time.Second,
				RequiredConcurrentSuccessful: int64(helper.CONFIG.CacheCircuitCloseRequiredCount),
			}),
			ClosedToOpenFactory: hystrix.OpenerFactory(hystrix.ConfigureOpener{
				RequestVolumeThreshold: int64(helper.CONFIG.CacheCircuitOpenThreshold),
			}),
		},
		Execution: circuit.ExecutionConfig{
			Timeout: 1 * time.Second,
		},
	})
}
