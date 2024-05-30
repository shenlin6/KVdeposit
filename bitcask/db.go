package bitcask

import (
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitcask.go/data"
	"bitcask.go/index"
)

// DB bitcask 存储引擎实例的结构体
type DB struct {
	option     Options                   // 用户自己配置的选项
	rwmu       *sync.RWMutex             // 读写互斥锁的结构体类型
	activeFile *data.DataFile            // 目前的活跃文件(只有一个)，可以写入
	oldFiles   map[uint32]*data.DataFile // 旧的数据文件（一个或者多个）
	index      index.Indexer             // 内存索引
	fileIDs    []int                     // 有序递增的fileID，只能用于加载索引使用（否则影响递增性）
	seqNum     uint64                    //事务的序列号，严格递增
}

// Open 打开 bitcask 储存引擎的实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验，避免破坏数据库内部操作
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 对用户传递过来的目录进行校验，如果目录不为空，但这个目录不存在（第一次使用），需要创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil { //递归创建目录 os.ModePerm 给予目录读写执行的权限
			return nil, err
		}
	}

	//初始化 DB 实例的结构体，对其数据结构进行初始化
	db := &DB{
		// 注意使用了引用的数据结构都需要 new 或者 make 一个空间
		option:   options,
		rwmu:     new(sync.RWMutex),
		oldFiles: make(map[uint32]*data.DataFile),
		index:    index.NewIndexer(options.IndexType), //用户自己选择索引类型（Btree ART）
	}

	// 加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引的方法
	if err := db.loadIndexFromFiles(); err != nil {
		return nil, err
	}

	//加载完成之后，返回DB的结构体实例
	return db, nil
}

// Put DB数据写入的方法：写入 Key(非空) 和 Value
func (db *DB) Put(key []byte, value []byte) error {
	//要写入的数据为空直接返回
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造 LogRecord 结构体实例
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeqNum(key, nonTransactionSeqNum),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//调用 appendLogRecord,追加写入当前的活跃文件中
	pos, err := db.appendLogRecordWithLock(&logRecord)
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
	// 读多写少 !
	db.rwmu.RLock()         //加锁保护
	defer db.rwmu.RUnlock() //读完数据之后释放锁

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

	//如果这个Key 存在于数据库中的话，返回用户实际储存的Value
	return db.getValueByPosition(logRecordPos)
}

// Close 关闭数据库
func (db *DB) Close() error {
	// 只需要关闭当前的活跃文件即可

	//为空直接返回
	if db.activeFile == nil {
		return nil
	}

	//加锁，保证没有其他并发操作干扰到数据库的状态
	db.rwmu.Lock()
	defer db.rwmu.Unlock()

	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	// 关闭所有旧的文件
	for _, file := range db.oldFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	//活跃文件为空就返回
	if db.activeFile == nil {
		return nil
	}

	//加锁，保证没有其他并发操作干扰到数据库的状态
	db.rwmu.Lock()
	defer db.rwmu.Unlock()

	//持久化当前的活跃文件
	return db.activeFile.Sync()
}

// 获取数据库中所有的 Key
func (db *DB) ListKeys() [][]byte {
	it := db.index.Iterator(false)
	defer it.Close()

	//获取到所有Key的一个列表
	keys := make([][]byte, db.index.Size())
	var index int

	//从索引的第一个位置开始,对 keys 进行遍历，拿出每一个key
	for it.Rewind(); it.Valid(); it.Next() {
		keys[index] = it.Key()
		index++
	}

	//返回所有的key的列表
	return keys
}

// 对数据库中所有的数据进行指定的操作(用户自行指定)
// 当返回 false 的时候就终止遍历
func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	//由于是用户操作，加锁
	db.rwmu.RLock()
	defer db.rwmu.RUnlock()

	//拿到迭代器
	it := db.index.Iterator(false)
	for it.Rewind(); it.Valid(); it.Next() {
		v, err := db.getValueByPosition(it.Value())
		if err != nil {
			return err
		}
		//拿到数据后，对该数据执行用户指定的方法
		if !f(it.Key(), v) { //如果返回的时false,说明我们应该退出遍历了
			break
		}
	}
	return nil
}

// getValueByPosition 通过索引信息获取到实际的 Value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	//根据文件ID找到对应数据文件
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
	logRecord, _, err := dataFile.ReadLogRecord(dataFile.Offsetnow)
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

// Delete 删除数据的方法
func (db *DB) Delete(key []byte) error {
	// 首先校验用户传入的Key,为空直接返回
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 再看用户这个Key是否存在，不存在也返回
	logRecordpos := db.index.Get(key)
	if logRecordpos == nil {
		return nil
	}

	// 开始删除,对logrecord进行删除标记，并存入数据文件中
	// 进行标记
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNum(key, nonTransactionSeqNum),
		Type: data.LogRecordDeleted}

	// 写入数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 在对应的内存索引中删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecordWithLock(为了在Commit的时候不发生锁的冲突，我们重新写一个带锁的逻辑)
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.rwmu.Lock()         //加锁
	defer db.rwmu.Unlock() //解锁

	return db.appendLogRecord(logRecord)
}

// appendLogRecord 构造 LogRecord append 的方法：数据文件的追加写入
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
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
		Offset: writeOff,
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

// loadDataFiles 数据库启动时：加载对应的数据文件
func (db *DB) loadDataFiles() error {
	//首先根据配置项读取存储的对应目录，拿到目录列表
	dirEntry, err := os.ReadDir(db.option.DirPath)
	if err != nil {
		return err
	}

	//参考网上资料:约定以 .data 为后缀的文件为目标数据文件
	var fileIDs []int
	for _, entry := range dirEntry { //entry: 其中的一个子目录
		if strings.HasSuffix(entry.Name(), data.DataFileSuffix) {
			//如果是以 .data 结尾的文件：对这个文件进行名称分割,拿到前半部分(0001.data -> 0001)
			splitName := strings.Split(entry.Name(), ".")
			//以 ASCII 码为中介将 string 解析为 int 类型,拿到对应的fileid
			fileID, err := strconv.Atoi(splitName[0])
			//解析错误说明数据目录可能被损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			//将对应的fileID分别存放到fileIDs中
			fileIDs = append(fileIDs, fileID)
		}
	}
	// 对 fileIDs进行排序，需要从小到大分别分别加载数据文件，保证递增性
	sort.Ints(fileIDs)
	db.fileIDs = fileIDs //赋值，使其实例化的同时满足有序

	//遍历每一个文件的ID,打开每一个对应的数据文件
	for i, fid := range fileIDs {
		datafile, err := data.OpenDataFile(db.option.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		//如果遍历到最新的文件(活跃文件)则停止，否则就将该文件加入旧文件队伍中（保证活跃文件的唯一性）
		//一个简单的算法：(注意 ! ! ! 从0开始索引)
		if i == len(fileIDs)-1 {
			db.activeFile = datafile
		} else { //加入旧文件
			db.oldFiles[uint32(fid)] = datafile
		}
	}

	return nil
}

// loadIndexFromFiles 从数据文件中加载索引的方法
func (db *DB) loadIndexFromFiles() error {
	// 遍历文件中的所有记录，并加载到内存的索引中去
	// 注意：! ! map无序，想要从小到大通过fileID来添加内存索引，需要复用loadDataFiles中有序的fileIDs

	//为空直接返回
	if len(db.fileIDs) == 0 {
		return nil
	}

	//定义更新内存索引的方法
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		//这个索引可能被删除，查看是否有墓碑值,有的话直接删除
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			//正常的话就加入内存索引
			ok = db.index.Put(key, pos)
		}
		if !ok {
			//启动时及时提醒用户
			panic("failed to update index when starting up")
		}
	}

	// 缓存我们事务的数据，等待一整批事务都完成之后再更新索引
	transactionRecords := make(map[uint64][]*data.TransactionRecord) //map[序列号]

	// 序列号从零开始
	var currentSeqNum = nonTransactionSeqNum

	// 遍历所有的文件ID,取出所有文件中的内容
	for i, fid := range db.fileIDs {
		//类型转换，方便处理活跃文件和旧文件
		var dataFile *data.DataFile
		fileID := uint32(fid)
		if fileID == db.activeFile.FileID {
			dataFile = db.activeFile
		} else { //旧文件
			dataFile = db.oldFiles[fileID]
		}

		// 拿到一个文件后，从 0 开始循环处理这个文件中的所有内容
		var offset int64 = 0
		// 遍历一个文件的所有内容
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果文件读完了，需要跳出循环
				if err == io.EOF {
					break
				} else {
					//如果是其他错误直接返回
					return err
				}
			}

			//构造内存索引并保存
			logRecordPos := data.LogRecordPos{Fid: fileID, Offset: offset}

			//解析Key,拿到对应的事务序列号
			realKey, seqNum := parseLogRecordKey(logRecord.Key)

			//判断我们的序列号是否是事务类型，非事务提交的话直接更新内存索引
			if seqNum == nonTransactionSeqNum {
				updateIndex(realKey, logRecord.Type, &logRecordPos)
			} else {
				//如果是 WriteBatch 的事务类型
				//事务完成之后，对应的数据更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {

					for _, txnRecord := range transactionRecords[seqNum] {

						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)

					}
					// 对map类型进行清理缓存，方便下次继续使用
					delete(transactionRecords, seqNum)
				} else {
					//  走到这里说明是在WriteBatch中写入的数据，但是目前还没有提交成功，先缓存起来
					logRecord.Key = realKey
					transactionRecords[seqNum] = append(transactionRecords[seqNum], &data.TransactionRecord{
						Record: logRecord,
						Pos:    &logRecordPos,
					})

				}

			}

			//标记最新的序列号，方便我们每一批事务都从最新的序列号开始
			if seqNum > currentSeqNum {
				currentSeqNum = seqNum
			}

			//对偏移量 offset 进行递增
			offset += size
		}
		//读取到活跃文件跳出循环之后:进行当前offset的更新，以便于下一次从这里开始写入数据
		if i == len(db.fileIDs)-1 {
			db.activeFile.Offsetnow = offset
		}
	}

	//更新事务的序列号
	db.seqNum = currentSeqNum

	return nil
}

// checkOptions 对用户传入的配置项进行校验
func checkOptions(options Options) error {
	// 如果用户传入的目录为空，直接返回
	if options.DirPath == "" {
		return ErrDirPathNil
	}
	//大小为0，同样返回
	if options.DataFileSize == 0 {
		return ErrDataFileSizeNil
	}

	return nil
}
