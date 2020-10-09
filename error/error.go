package error

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/journeymidnight/yig/helper"
	"github.com/journeymidnight/yig/log"
)

type InternalError interface {
	error
	ErrorCode() InternalErrorCode
	Description() string
	ErrorID() string
}

func NewError(code InternalErrorCode, description string, err error) InternalError {
	return newInternalError(code, description, err)
}

func SprintError(code, errID, message, extra string, origErr error) string {
	msg := fmt.Sprintf("%s %s: %s", errID, code, message)
	if extra != "" {
		msg = fmt.Sprintf("%s\t%s", msg, extra)
	}
	if origErr != nil {
		msg = fmt.Sprintf("%s\ncaused by: %v", msg, origErr)
	}
	return msg
}

type InternalErrorCode int

const (
	// Fatal error
	InternalFatalError InternalErrorCode = iota
	InCephFatalError
	InTidbFatalError
	InTikvFatalError
	InRedisFatalError
	InIamFatalError
	InMetaFatalError
	InDatatypeFatalError
	InSignatureFatalError
	FlagOfFatalError
	// General error
	InRedisGeneralError
	InIamGeneralError
	InMetaGeneralError
	InDatatypeGeneralError
	InSignatureGeneralError
	FlagOfGeneralError

	// Warn
	InCryptoWarn
	InMetaWarn
	FlagOfWarn
)

var ErrorCode = map[InternalErrorCode]string{
	InternalFatalError:    "InternalFatalError",
	InTidbFatalError:      "InternalTidbFatalError",
	InTikvFatalError:      "InternalTikvFatalError",
	InCephFatalError:      "InternalCephFatalError",
	InRedisFatalError:     "InternalRedisFatalError",
	InIamFatalError:       "InternalIamFatalError",
	InMetaFatalError:      "InternalMetaFatalError",
	InDatatypeFatalError:  "InternalDatatypeFatalError",
	InSignatureFatalError: "InternalSignatureFatalError",
	FlagOfFatalError:      "InternalFlagOfFatalException",

	InRedisGeneralError:     "InternalRedisGeneralError",
	InIamGeneralError:       "InternalIamGeneralError",
	InMetaGeneralError:      "InternalMetaGeneralError",
	InDatatypeGeneralError:  "InternalDatatypeGeneralError",
	InSignatureGeneralError: "InternalSignatureGeneralError",
	FlagOfGeneralError:      "InternalFlagOfGeneralException",

	InCryptoWarn: "InternalCryptoWarn",
	InMetaWarn:   "InternalMetaWarn",
	FlagOfWarn:   "InternalFlagOfWarnException",
}

type internalError struct {
	code        InternalErrorCode
	description string
	err         error
	errorId     string
	location    string
}

func newInternalError(code InternalErrorCode, description string, err error) *internalError {
	i := &internalError{
		code:        code,
		description: description,
		err:         err,
		errorId:     string(helper.GenerateRandomId()),
		location:    getCaller(3),
	}
	return i
}

func (i internalError) Error() string {
	return SprintError(i.ErrorCodeInfo(), i.errorId, i.description, i.location, i.err)
}

func (i internalError) ErrorCodeInfo() string {
	errCode, ok := ErrorCode[i.code]
	if !ok {
		return "InternalError"
	}
	return errCode
}

func (i internalError) ErrorCode() InternalErrorCode {
	return i.code
}

func (i internalError) Description() string {
	return i.description
}

func (i internalError) ErrorID() string {
	return i.errorId
}

func getCaller(skipCallDepth int) string {
	_, fullPath, line, ok := runtime.Caller(skipCallDepth)
	if !ok {
		return ""
	}
	fileParts := strings.Split(fullPath, "/")
	file := fileParts[len(fileParts)-2] + "/" + fileParts[len(fileParts)-1]
	return fmt.Sprintf("Error location: %s:%d", file, line)
}

func ParseError(err error) (error, log.Level) {
	if e, ok := err.(InternalError); ok {
		if e.ErrorCode() < FlagOfFatalError {
			return ErrInternalError, log.FatalLevel
		} else if e.ErrorCode() < FlagOfGeneralError {
			return ErrInternalError, log.ErrorLevel
		} else {
			return ErrInternalError, log.WarnLevel
		}
	} else if _, ok := err.(ApiError); ok {
		return err, log.WarnLevel
	}
	return err, log.ErrorLevel
}
