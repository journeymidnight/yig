package glacierclient

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/client"
	. "github.com/journeymidnight/yig/error"
)

// To upload an archive to a vault.
func (c GlacierClient) PutArchive(accountid, vaultname string, ioreadseeker io.ReadSeeker) (*string, error) {
	input := &glacier.UploadArchiveInput{
		AccountId:          aws.String(accountid),
		ArchiveDescription: aws.String("-"),
		Body:               ioreadseeker,
		Checksum:           aws.String(""),
		VaultName:          aws.String(vaultname),
	}
	result, err := c.Client.UploadArchive(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeRequestTimeoutException:
				err = ErrRequestTimeout
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	archiveid := result.ArchiveId
	return archiveid, err
}

//To delete an archive from a vault.
func (c GlacierClient) DeleteArchive(accountid string, archiveid string, vaultname string) error {
	input := &glacier.DeleteArchiveInput{
		AccountId: aws.String(accountid),
		ArchiveId: aws.String(archiveid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.DeleteArchive(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeResourceNotFoundException:
				err = ErrResourceNotFound
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeServiceUnavailableException:
				err = ErrServiceUnavailable
			default:
				Logger.Println(5, "With error: ", aerr.Error())
			}
		} else {
			Logger.Println(5, "With error: ", aerr.Error())
		}
	}
	return err
}
