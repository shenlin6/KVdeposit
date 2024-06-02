package bitcask

import "errors"

//定义一些 error 的类型(参考了GitHub项目上的错误类型的定义)

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to updata index")
	ErrKeyNotFound            = errors.New("the key is not in the database")
	ErrDataFileNotFound       = errors.New("the datafile is not in the database")
	ErrDirPathNil             = errors.New("database dir path is empty")
	ErrDataFileSizeNil        = errors.New("data file size must greater than zero ")
	ErrDataDirectoryCorrupted = errors.New("the database directory may be corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max num of batch")
	ErrIsMergeNow             = errors.New("merge is in the process")
	ErrFilelockIsInUse        = errors.New("filelock is in use")
	ErrInvalidMergeRatio      = errors.New("invalid merge ratio")
	ErrUnderMergeRatio        = errors.New("the merge ratio now is under the merge ratio you set")
	ErrNotEnoughSpaceToMerge  = errors.New("no enough space to merge")
)
