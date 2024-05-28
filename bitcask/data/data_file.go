package data

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"

	"bitcask.go/fio"
)

const DataFileSuffix = ".data"

var (
	//自定义错误信息：
	ErrInvalidCRC = errors.New("crc value invalid,log record maybe corrupted")
)

// DataFile 数据文件的结构体
type DataFile struct {
	FileID    uint32        //文件id
	Offsetnow int64         //文件现在的偏移量（目前写到哪个位置了）
	IOManager fio.IOManager //实际对于数据的操作（io读写）
}

// OpenDataFile 打开新的数据文件 (需要用户传入目录的路径)
func OpenDataFile(dirPath string, fileid uint32) (*DataFile, error) {
	// 根据 dirPath 和 fileid 生成完整文件名称(需要加上后缀)
	fileName := filepath.Join(fmt.Sprintf("%09d", fileid) + DataFileSuffix)

	//拿到 IOManager 的对象
	ioManager, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	// 初始化数据文件
	return &DataFile{
		FileID:    fileid,
		Offsetnow: 0,
		IOManager: ioManager,
	}, nil
}

// ReadLogRecord 根据偏移量读取相应的数据文件
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	//拿到当前文件的大小
	fileSize, err := df.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize //(15)
	//如果当前的偏移量+15字节超过当前文件的大小，我们读取到文件末尾即可，否则会造成io.EOF错误
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}

	// 首先从数据文件当中根据 offset 拿到头部信息（按照最大头部信息字节数读取）
	headerbuf, err := df.readBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	// 对 headerbuf 进行解码
	header, headerSize := decodeLogRecordHeader(headerbuf)

	// 如果没有读取到 header，说明我们从数据文件中已经读取完了
	if header == nil {
		return nil, 0, io.EOF
	}

	//如果读取的 校验值 key_size 和 value_size 都为 0 ,也表示读取到了文件末尾，直接返回
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	// 取出对应 key 和 value 的长度
	var logRecordSize int64

	// 实例化一个 logRecord 的结构体，方便返回
	var logRecord = &LogRecord{Type: header.recordType}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	//记录整个logRecord的长度 = header 的长度 + keySize + valueSize
	logRecordSize = headerSize + int64(keySize) + int64(valueSize)

	// 根据 keySize 和 valueSize 读取用户实际读取的 key 和 value
	// 如果 size 确实大于 0 就读取出来
	if keySize > 0 || valueSize > 0 {
		//注意: ! ! ! 这里需要从当前偏移量再加上 headerSize 后面开始读取，否则读取的文件不完整！！！
		kvBuf, err := df.readBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// kvBuf 即为用户实际存储的数据，取出来
		logRecord.Key = kvBuf[:keySize]   // keysize长度之前的值
		logRecord.Value = kvBuf[keySize:] // keysize长度之后的值
	}
	// 校验 crc 的值是否正确,判断和header中的 crc的值是否完全相等，不相等则说明数据文件可能有乱码（被破坏）
	//注意: ! ! ! 不能把整个 headerbuf 切片全传进去,因为我们设置了最大头部信息字节数,
	//但实际的长度绝大多数时间没这么长,除非刚好巧合 = maxLogRecordHeaderSize，我们需要截取不需要的长度

	crc := getLogRecordCrc(logRecord, headerbuf[crc32.Size:headerSize]) //取从 crc32.Size 到 headerSize-1 的部分
	// 将LogRecord中的CRC与数据文件中的CRC进行比较，检验数据的有效性（是否被损坏）
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	//检验通过表示读取到的数据是有效的，进行返回
	return logRecord, logRecordSize, nil
}

// 其他的方法，比如: 在数据文件中写入数据，Sync Close 等方法，直接调用 IOMAnager 就可以了

// Sync 持久化操作
func (df *DataFile) Sync() error {
	return df.IOManager.Sync()
}

// Close 关闭文件
func (df *DataFile) Close() error {
	return df.IOManager.Close()
}

// Write 写入操作
func (df *DataFile) Write(buf []byte) error {
	//更新我们现在文件写道哪里了（更新偏移量：Offsetnow）
	n, err := df.IOManager.Write(buf)
	if err != nil {
		return err
	}

	df.Offsetnow += int64(n)
	return nil
}

// readBytes 读取n个字节,返回一个字节数组
func (df *DataFile) readBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	df.IOManager.Read(b, offset)
	return
}
