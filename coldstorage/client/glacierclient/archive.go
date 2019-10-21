package glacierclient

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/conf"
	. "github.com/journeymidnight/yig/error"
	"io"
)

// To upload an archive to a vault.
func (c GlacierClient) PutArchive(gc GlacierConf, accountid string, ioreadseeker io.ReadSeeker, vaultname string) (*glacier.ArchiveCreationOutput, error) {
	s3Config := Tos3Config(gc)
	newSession, _ := session.NewSession(s3Config)
	svc := glacier.New(newSession)
	input := &glacier.UploadArchiveInput{
		AccountId:          aws.String(accountid),
		ArchiveDescription: aws.String("-"),
		Body:               ioreadseeker,
		Checksum:           aws.String(""),
		VaultName:          aws.String(vaultname),
	}
	result, err := svc.UploadArchive(input)
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
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr.Error to get the Code and Message from an error.
			fmt.Println(err.Error())
		}
	}
	return result, err
}

//To delete an archive from a vault.
func (c GlacierClient) DelArchive(gc GlacierConf, accountid string, archiveid string, vaultname string) (*glacier.DeleteArchiveOutput, error) {
	s3Config := Tos3Config(gc)
	newSession, _ := session.NewSession(s3Config)
	svc := glacier.New(newSession)
	input := &glacier.DeleteArchiveInput{
		AccountId: aws.String(accountid),
		ArchiveId: aws.String(archiveid),
		VaultName: aws.String(vaultname),
	}
	result, err := svc.DeleteArchive(input)
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
				fmt.Println(aerr.Error())
			}
		} else {
			// Print the error, cast err to awserr. Error to get the Code and Message from an error.
			fmt.Println(err.Error())
		}
	}
	return result, err
}
