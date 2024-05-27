package bitcask

// Options 用户自己配置选项的结构体
type Options struct {
	DirPath string

	//每一个数据文件的大小
	DataFileSize int64

	//每次写入是否持久化
	SyncWrites bool
}
