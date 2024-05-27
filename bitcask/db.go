package bitcask

import (
	"sync"

	"bitcask.go/data"
	"bitcask.go/index"
)

// DB bitcask 存储引擎实例的结构体
type DB struct {
	option     Options                   //用户自己配置的选项
	rwmu       *sync.RWMutex             //读写互斥锁的结构体类型
	activeFile *data.DataFile            // 目前的活跃文件(只有一个)，可以写入
	oldFiles   map[uint32]*data.DataFile //旧的数据文件（一个或者多个）
	index      index.Indexer             //内存索引
}

// Put DB数据写入的方法：写入 Key(非空) 和 Value
func (db *DB) Put(key []byte, value []byte) error {
	//要写入的数据为空直接返回
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体实例
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//调用 appendLogRecord,追加写入当前的活跃文件中
	pos, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}

	//拿到索引信息之后，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	//没问题就直接返回
	return nil
}

// Get DB数据读取的方法
func (db *DB) Get(key []byte) ([]byte, error) {
	db.rwmu.Lock()         //加锁保护
	defer db.rwmu.Unlock() //读完数据之后释放锁

	//为空直接返回
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//从内存中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	//为空则说明没有这个 key 不在数据库中
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//有的话，根据文件ID找到对应数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileID == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		//通过在旧文件中索引找到相应的数据文件
		dataFile = db.oldFiles[logRecordPos.Fid]
	}

	//如果数据文件为空,说明没有这个数据文件
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}
	//如果不为空，说明找到了对应的数据文件，根据偏移量读取对应数据
	logRecord, err := dataFile.ReadLogRecord(dataFile.Offsetnow)
	if err != nil {
		return nil, err
	}

	//类型判断，判断该文件是否已经被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	//返回实际的数据
	return logRecord.Value, err

}

// appendLogRecord 构造 LogRecord append 的方法
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.rwmu.Lock() //加锁
	defer db.rwmu.Unlock()

	//判断当前的活跃文件是否存在(因为数据库没有写入的之前没有文件生成)，将其初始化
	//如果活跃文件为空则初始化该文件
	if db.activeFile == nil {
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	// 开始对当前数据文件进行读写操作
	encRecord, size := data.EncodeLogRecord(logRecord) //拿到一个编码后的结果和长度
	//注意! ! ! 写入之前判断:当前活跃文件大小再加上需要写入的数据的大小是否超过阈值，
	//超过则改变目前活跃文件的状态并且需要新打开一个活跃文件，然后再写入新的活跃文件
	if db.activeFile.Offsetnow+size > db.option.DataFileSize {
		//将当前活跃的文件进行持久化（保证安全持久化进入磁盘）
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//将之前的活跃文件持久化之后，将其转化为旧的数据文件，以便于更新活跃文件
		db.oldFiles[db.activeFile.FileID] = db.activeFile

		//打开新的数据文件作为新的活跃文件
		if err := db.setActiveFile(); err != nil {
			return nil, err
		}
	}
	//开始实现数据文件写入操作：
	//记录当前的offset
	writeOff := db.activeFile.Offsetnow
	//将编码后的文件写入新的活跃文件
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//提供配置项，让用户自行判断是否需要安全的持久化
	if db.option.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//构造内存索引信息，返回去上一层
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: uint64(writeOff),
	}
	return pos, nil
}

// setActiveFile 设置当前活跃文件
// 注意！！！使用这个 DB 方法的时候必须持有互斥锁，不然并发访问会崩
func (db *DB) setActiveFile() error {
	var defaultFileID uint32 = 0 //默认初始化为0

	if db.activeFile != nil { //如果目前活跃文件不为空，则初始化的时候Fileid+1(保证Fileid递增)
		defaultFileID = db.activeFile.FileID + 1
	}
	//打开新的数据文件作为活跃文件
	datafile, err := data.OpenDataFile(db.option.DirPath, defaultFileID)
	if err != nil {
		return err
	}
	db.activeFile = datafile //修改目前的活跃文件

	return nil
}
