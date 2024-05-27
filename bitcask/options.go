package bitcask

// Options 用户自己配置选项的结构体
type Options struct {
	DirPath string

	//每一个数据文件的大小
	DataFileSize int64

	//每次写入是否持久化
	SyncWrites bool

	// 索引类型
	IndexType IndexerType
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1 //从1开始计数

	// ART 自适应树
	APT
)
