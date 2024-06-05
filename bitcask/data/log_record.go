package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal      LogRecordType = iota //正常操作的类型
	LogRecordDeleted                          //针对被删除的文件操作的类型
	LogRecordTxnFinished                      //标识事务提交的类型

	// 包括 crc校验值(4字节) 、Type类型(1字节)、Key 的大小、Value 的大小 (这两个为动态长度，节约内存)
	// 4 + 1 + 5 + 5 =15
	maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5 //最大头部信息字节数：15
)

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件的id 表示数据存放到了哪一个文件中
	Offset int64  //偏移量，表示将该数据存放到了文件中哪个位置
	Size   uint32 //数据在磁盘上的大小
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

// TransactionRecord 缓存事务类型的相关数据
type TransactionRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// EncodeLogRecord 对 LogRecord 进行编码，返回字节数组及长度

// LogRecord图 如下：
//	（ crc 校验值 ） （  type 类型 ）  （   key size )    (value size )        (  key  )    (     value   )
//	    4字节           1字节          动态长度（max:5）     动态长度（max:5）    动态长度         动态长度

// EncodeLogRecord 编码: 数据文件写入时需要将对应结构体解码转为字符数组类型（切片）
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// 首先将header部分编码写入字节数组中
	header := make([]byte, maxLogRecordHeaderSize)

	// 因为 CRC 需要在后面数据的字节确定之后才能计算，需要先从第五个字节开始写(注意从0开始索引)
	header[4] = logRecord.Type
	var index = 5
	// 从index(第5个字节)之后，存储的是K V的长度

	// 写入 Key 的长度
	// 使用Go 语言标准库中的函数，将有符号整数编码为可变长度的字节序列(可变长度可节约空间)
	// 返回写入多少字节,更新目前的索引位置
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	//写入 Value 的长度
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	//记录完了k/v的长度（实际的长度，但我们设定的长度为5/每个key或者value），因此这里的Index很有可能
	//小于Key的位置，因为要返回，所以需要手动调整 index 保证编码正确性

	// size  加上key_size和value_size之后的长度：实际编码后的长度
	var size = index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	//将可能没使用完的header也拷贝过来
	copy(encBytes[:index], header[:index])

	// key 和 value 本来就是字节数组，直接加进来
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	//拷贝完之后，对整个 logRecord 进行CRC校验(从第五个字节开始，从零索引)，判断数据是否有效
	crc := crc32.ChecksumIEEE(encBytes[4:])

	//拿到 crc 之后放进前四个字节(小端序插入)
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	return encBytes, int64(size) //返回一整条 logRecord 和其长度
}

// 对索引信息进行编码的方法
func EncodeLogRecordPos(pos *LogRecordPos) []byte {

	buf := make([]byte, binary.MaxVarintLen32*2, binary.MaxVarintLen64)
	var index = 0
	//编码文件ID
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	//编码偏移量
	index += binary.PutVarint(buf[index:], pos.Offset)
	//编码数据大小
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// 对索引信息进行解码的方法
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	// 依次将fileID和offset取出来
	var index = 0
	fileID, n := binary.Varint(buf[index:])
	index += n

	offset, n:= binary.Varint(buf[index:])
	index += n

	size, _ := binary.Varint(buf[index:])

	// 构造出索引信息并返回
	return &LogRecordPos{
		Fid:    uint32(fileID),
		Offset: offset,
		Size:   uint32(size),
	}

}

// 对 headerbuf 进行解码的方法,返回 header 的实际的头部信息和长度
func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	//如果连CRC的四个字节长度都没达到，直接返回
	if len(buf) <= 4 {
		return nil, 0
	}

	// 从前往后依次拿出来所有的数据
	header := &logRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]), //拿四个字节，但从第五个字节开始索引
		recordType: buf[4],
	}

	//从第五个字节开始拿出后面的数据,更新index,取出实际的数据
	var index = 5

	keySize, n := binary.Varint(buf[index:])
	header.keySize = uint32(keySize)
	index += n

	// 取出实际的 value size
	valueSize, n := binary.Varint(buf[index:])
	header.valueSize = uint32(valueSize)
	index += n

	//目前的index代表实际header的长度,返回到上一层
	return header, int64(index)
}

// 定义一个获取 crc 值的方法
func getLogRecordCrc(logRecord *LogRecord, header []byte) uint32 {
	// 判断这条数据记录是否为空
	if logRecord == nil {
		return 0
	}

	//进行 header 部分的crc校验
	crc := crc32.ChecksumIEEE(header[:])
	//然后分别更新 key 和 value 部分的 crc 校验值
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Key) // crc32.IEEETable :快速计算 CRC-32 校验值的
	crc = crc32.Update(crc, crc32.IEEETable, logRecord.Value)

	return crc //实际最后计算出来的CRC
}
