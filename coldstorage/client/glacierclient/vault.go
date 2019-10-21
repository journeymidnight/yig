package glacierclient

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/conf"
	. "github.com/journeymidnight/yig/error"
)

//To create a new vault with the specified name.
func (c GlacierClient) CreatVault(gc GlacierConf, accountid string, vaultname string) (*glacier.CreateVaultOutput, error) {
	s3Config := Tos3Config(gc)
	newSession, _ := session.NewSession(s3Config)
	svc := glacier.New(newSession)
	input := &glacier.CreateVaultInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	result, err := svc.CreateVault(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case glacier.ErrCodeInvalidParameterValueException:
				err = ErrInvalidParameterValue
			case glacier.ErrCodeMissingParameterValueException:
				err = ErrMissingParameterValue
			case glacier.ErrCodeLimitExceededException:
				err = ErrLimitExceeded
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
