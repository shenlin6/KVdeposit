package fio

const DataFilePerm = 8644

const (
	// StandardFIO 标准文件 IO
	StandardFIO FileIOType = iota

	// MemoryMap 内存文件映射
	MemoryMap
)

type FileIOType = byte

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

// FileID 标准系统文件 ID

// NewIOManager 初始化 IOManager 的方法,根据用户传递的IO类型进行选择
func NewIOManager(fileName string, IOType FileIOType) (IOManager, error) {
	switch IOType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)

	default:
		panic("unsuported io type")
	}
}
