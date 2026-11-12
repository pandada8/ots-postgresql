package ots

import "fmt"

const (
	CodeParameterInvalid     = "OTSParameterInvalid"
	CodeAuthFailed           = "OTSAuthFailed"
	CodeObjectNotExist       = "OTSObjectNotExist"
	CodeObjectAlreadyExist   = "OTSObjectAlreadyExist"
	CodeConditionCheckFail   = "OTSConditionCheckFail"
	CodeRowOperationConflict = "OTSRowOperationConflict"
	CodeInternalServerError  = "OTSInternalServerError"
	CodeMethodNotAllowed     = "OTSMethodNotAllowed"
	CodeRequestBodyTooLarge  = "OTSRequestBodyTooLarge"
	CodeOutOfRowSizeLimit    = "OTSOutOfRowSizeLimit"
	CodeOutOfColumnSizeLimit = "OTSOutOfColumnSizeLimit"
	CodeStorageServerBusy    = "OTSStorageServerBusy"
	CodeTableNotReady        = "OTSTableNotReady"
	CodeUnsupported          = "OTSNotSupportedYet"
)

type Error struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func newError(code string, status int, format string, args ...interface{}) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...), HTTPStatus: status}
}

func ErrParameterInvalid(format string, args ...interface{}) *Error {
	return newError(CodeParameterInvalid, 400, format, args...)
}

func ErrAuthFailed(format string, args ...interface{}) *Error {
	return newError(CodeAuthFailed, 403, format, args...)
}

func ErrObjectNotExist(format string, args ...interface{}) *Error {
	return newError(CodeObjectNotExist, 404, format, args...)
}

func ErrObjectAlreadyExist(format string, args ...interface{}) *Error {
	return newError(CodeObjectAlreadyExist, 409, format, args...)
}

func ErrConditionCheckFail(format string, args ...interface{}) *Error {
	return newError(CodeConditionCheckFail, 403, format, args...)
}

func ErrRowOperationConflict(format string, args ...interface{}) *Error {
	return newError(CodeRowOperationConflict, 409, format, args...)
}

func ErrInternal(format string, args ...interface{}) *Error {
	return newError(CodeInternalServerError, 500, format, args...)
}

func ErrUnsupported(format string, args ...interface{}) *Error {
	return newError(CodeUnsupported, 400, format, args...)
}

func ErrOutOfColumnSizeLimit(format string, args ...interface{}) *Error {
	return newError(CodeOutOfColumnSizeLimit, 400, format, args...)
}

func AsError(err error) *Error {
	if err == nil {
		return nil
	}
	if otsErr, ok := err.(*Error); ok {
		return otsErr
	}
	return ErrInternal("%s", err.Error())
}
