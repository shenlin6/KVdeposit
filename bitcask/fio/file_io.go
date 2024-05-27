package fio

const DataFilePerm =8644


//抽象 IO 管理器的接口 可以接入不同的IO类型（标准IO文件）
type IDManager interface {
	// Read 从对应位置读取数据
	Read([]byte, int64) (int, error)

	// Write 写入字节级别文件中
	Write([]byte) (int, error)
	
	// Sync 持久化数据
	Sync() error
	
	// Close 关闭文件
	Close() error
}
