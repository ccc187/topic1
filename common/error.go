package common

type InternalError struct {
	ErrCode int32
	ErrMsg  string
}

func (e *InternalError) Error() string {
	return e.ErrMsg
}

