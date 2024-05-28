package fio

const DataFilePerm = 8644

// 抽象 IO 管理器的接口 可以接入不同的IO类型（标准IO文件）
type IOManager interface {
	// Read 从对应位置读取数据
	Read([]byte, int64) (int, error)

	// Write 写入字节级别文件中
	Write([]byte) (int, error)

	// Sync 持久化数据
	Sync() error

	// Close 关闭文件
	Close() error

	// Size 获取剩余文件大小
	Size() (int64, error)
}

// NewIDManager 初始化 IDManager 的方法,目前只能传入标准的 fileName
func NewIOManager(fileName string) (IOManager, error) {
	return NewFileIOManager(fileName)
}
