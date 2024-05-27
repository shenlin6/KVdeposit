package data

// LogRecordPos 数据内存索引，描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 //文件的id 表示数据存放到了哪一个文件中
	Offset uint64 //偏移量，表示将该数据存放到了文件中哪个位置
}
