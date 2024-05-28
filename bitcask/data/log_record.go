package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal  LogRecordType = iota //正常操作的类型
	LogRecordDeleted                      //针对被删除的文件操作的类型
	

	// 包括 crc校验值(4字节) 、Type类型(1字节)、Key 的大小、Value 的大小 (这两个为动态长度，节约内存)
	// 4 + 1 + 5 + 5 =15
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 //最大头部信息字节数：15
)

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件的id 表示数据存放到了哪一个文件中
	Offset int64  //偏移量，表示将该数据存放到了文件中哪个位置
}

// 写入到数据文件的记录（因为是添加写入，所以可以类似看作日志）
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType //墓碑值，可用于标记删除
}

// logRecordHeader header 的信息
type logRecordHeader struct {
	crc        uint32        // crc校验值(4字节) //注意: ! ! ! 相同类型放在一起，节约内存！
	keySize    uint32        // Key 的大小
	valueSize  uint32        // Value 的大小
	recordType LogRecordType // Type类型(1字节)
}

// EncodeLogRecord 数据文件写入时需要将对应结构体解码转为字符数组类型（切片）
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	return nil, 0 //待会再写逻辑
}

// 对 headerbuf 进行解码的方法,返回 header 的头部信息和长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	return nil, 0 //待会儿来实现
}

// 定义一个获取 crc 值的方法
func getLogRecordCrc(logRecord *LogRecord, header []byte) uint32 {
	return 0 //待会再来写
}
