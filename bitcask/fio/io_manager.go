package fio

import "os"

// FileID 标准系统文件 ID

//对golang标准的文件操作进行封装

type FileID struct {
	fd *os.File //系统文件描述码
}

// NewFileIDManager 初始化标准文件 ID
func NewFileIDManager(filename string) (*FileID, error) {
	fid, err := os.OpenFile(
		filename,
		os.O_CREATE|os.O_APPEND, //没有则创建，只允许追加写入
		DataFilePerm,            //文件所有者可写可读，其他用户只可读
	)
	if err != nil {
		return nil, err
	}
	return &FileID{fd: fid}, nil
}

//实现 IDManager 几个接口

func (fio *FileID) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileID) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileID) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileID) Close() error {
	return fio.fd.Close()
}
