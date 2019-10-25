package client

import (
	"io"

	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	"github.com/journeymidnight/yig/log"
)

var Logger *log.Logger

func InitiateColdstorageClient(logger *log.Logger) {
	Logger = logger
}

//Cold storage Client Interface
type Client interface {
	//archives
	PutArchive(accountid, vaultname string, ioreadseeker io.ReadSeeker) (*string, error)
	DeleteArchive(accountid string, archiveid string, vaultname string) error

	//vaults
	CreateVault(accountid string, vaultname string) error
	GetVaultInfo(accountid string, vaultname string) (*VaultInfo, error)
	DeleteVault(accountid string, vaultname string) error

	//job
	PostJob(accountid string, jobpara *glacier.JobParameters, vaultname string) (*string, error)
	GetJobStatus(accountid string, jobid string, vaultname string) (*JobStatus, error)
	GetJobOutput(accountid string, jobid string, vaultname string) (io.ReadCloser, error)
}
