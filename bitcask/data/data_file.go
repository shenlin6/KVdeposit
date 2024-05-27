package data

import "bitcask.go/fio"

// DataFile 数据文件的结构体
type DataFile struct {
	FileID    uint32        //文件id
	Offsetnow int64         //文件现在的偏移量（目前写到哪个位置了）
	IdManager fio.IDManager //实际对于数据的操作（io读写）
}

// OpenDataFile 打开新的数据文件 (需要用户传入目录的路径)
func OpenDataFile(dirPath string, fileid uint32) (*DataFile, error) {
	return nil, nil
}

// ReadLogRecord 根据偏移量读取相应的数据文件
func (df *DataFile) ReadLogRecord(offser int64) (*LogRecord, error) {
	return nil, nil //待会儿来实现
}

// Sync 持久化操作
func (df *DataFile) Sync() error {

}

// Write 写入操作
func (df *DataFile) Write(buf []byte) error {
	return nil //待会儿再来完成逻辑处理
}
