package client

import (
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	"io"
)

//Cold storage Client Interface
type Client interface {
	//archives
	PutArchive(accountid, vaultname string, ioreadseeker io.ReadSeeker) (*string, error)
	DelArchive(accountid string, archiveid string, vaultname string) error

	//vaults
	CreatVault(accountid string, vaultname string) error
	GetVaultInfo(accountid string, vaultname string) (*VaultInfo, error)
	DelVault(accountid string, vaultname string) error

	//job
	PostJob(accountid string, jobpara *glacier.JobParameters, vaultname string) (*string, error)
	GetJobStatus(accountid string, jobid string, vaultname string) (*JobStatus, error)
	GetOutput(accountid string, jobid string, vaultname string) (io.ReadCloser, error)
}
