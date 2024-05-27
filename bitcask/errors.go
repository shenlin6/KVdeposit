package bitcask

import "errors"

//定义一些 error 的类型

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("failed to updata index")
	ErrKeyNotFound       = errors.New("the key is not in the database")
	ErrDataFileNotFound  = errors.New("the datafile is not in the database")
)
