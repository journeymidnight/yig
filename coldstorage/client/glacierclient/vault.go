package glacierclient

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/glacier"
	. "github.com/journeymidnight/yig/coldstorage/types/glaciertype"
	. "github.com/journeymidnight/yig/error"
	"github.com/journeymidnight/yig/helper"
)

//To create a new vault with the specified name.
func (c GlacierClient) CreateVault(accountid string, vaultname string) error {
	input := &glacier.CreateVaultInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.CreateVault(input)
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
			helper.Logger.Println(5, "Internal error!")
		}
	}
	return err
}

//To return information about a vault.
func (c GlacierClient) GetVaultInfo(accountid string, vaultname string) (*VaultInfo, error) {
	input := &glacier.DescribeVaultInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	result, err := c.Client.DescribeVault(input)
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
			helper.Logger.Println(5, "Internal error!")
		}
	}
	vaultinfo := &VaultInfo{
		NumberOfArchives: result.NumberOfArchives,
	}
	return vaultinfo, err
}

//To deletes a vault with the specified name.
func (c GlacierClient) DeleteVault(accountid string, vaultname string) error {
	input := &glacier.DeleteVaultInput{
		AccountId: aws.String(accountid),
		VaultName: aws.String(vaultname),
	}
	_, err := c.Client.DeleteVault(input)
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
			helper.Logger.Println(5, "Internal error!")
		}
	}
	return err
}
