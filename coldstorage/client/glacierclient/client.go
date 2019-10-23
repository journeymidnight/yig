package glacierclient

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/conf"
)

type GlacierClient struct {
	Client *glacier.Glacier
}

func NewGlacierClient(endpoint, region, ak, sk string) *GlacierClient {
	SessConfig := ToSessConfig(endpoint, region, ak, sk)
	newSession, _ := session.NewSession(SessConfig)
	glac := glacier.New(newSession)
	gclient := &GlacierClient{
		Client: glac,
	}
	return gclient
}
