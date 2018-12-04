package v2

import (
	"github.com/journeymidnight/yig/iam/common"
	"github.com/journeymidnight/yig/circuitbreak"
)

type Client struct {
	httpClient *circuitbreak.CircuitClient
}

func (a Client) GetKeysByUid (string) (credentials []common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClient()
	}
	return
}

func (a Client) GetCredential (string) (credential common.Credential, err error) {
	if a.httpClient == nil {
		a.httpClient = circuitbreak.NewCircuitClient()
	}
	return
}