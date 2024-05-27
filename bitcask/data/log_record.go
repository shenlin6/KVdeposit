package data

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota //正常操作的类型
	LogRecordDeleted                      //针对被删除的文件操作的类型
)

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件的id 表示数据存放到了哪一个文件中
	Offset int64 //偏移量，表示将该数据存放到了文件中哪个位置
}

// 写入到数据文件的记录（因为是添加写入，所以可以类似看作日志）
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //墓碑值，可用于标记删除
}

// EncodeLogRecord 数据文件写入时需要将对应结构体编码转为字符数组类型（切片）
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0 //待会再写逻辑
}
