package bitcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"bitcask.go/data"
	"bitcask.go/fio"
	"bitcask.go/index"
	"bitcask.go/utils"
	"github.com/gofrs/flock"
)

const (
	seqNumKey    = "seq.num"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例的结构体
type DB struct {
	option           Options                   // 用户自己配置的选项
	rwmu             *sync.RWMutex             // 读写互斥锁的结构体类型
	activeFile       *data.DataFile            // 目前的活跃文件(只有一个)，可以写入
	oldFiles         map[uint32]*data.DataFile // 旧的数据文件（一个或者多个）
	index            index.Indexer             // 内存索引
	fileIDs          []int                     // 有序递增的fileID，只能用于加载索引使用（否则影响递增性）
	seqNum           uint64                    //事务的序列号，严格递增
	isMerging        bool                      //标识这个时刻有无merge正在进行(只允许一个)
	seqNumFileExists bool                      //标识是否有事务序列号的文件
	isNewInitial     bool                      //判断是否是第一次初始化数据文件的用户
	fileLock         *flock.Flock              //文件锁，保证多进程之间互斥
	bytesWrite       uint                      //当前写了多少字节
	reclaimSize      int64                     //标识有多少无效数据
}

type Stat struct {
	KeyNum      uint  //Key的总数量
	DataFileNum uint  //数据文件的总数量
	reclaimSize int64 //可以回收的数据量,以字节为单位
	DiskSize    int64 //数据目录所占磁盘空间的大小
}

// BackUp 拷贝数据库的方法(dir 用户传递过来的需要拷贝的目标目录)
func (db *DB) BackUp(dir string) error {
	db.rwmu.RLock()
	defer db.rwmu.RUnlock()

	return utils.CopyDir(db.option.DirPath, dir, []string{fileLockName})
}

// Open 打开 bitcask 储存引擎的实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验，避免破坏数据库内部操作
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isNewInitial bool

	// 对用户传递过来的目录进行校验，如果目录不为空，但这个目录不存在（第一次使用），需要创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {

		isNewInitial = true //第一次初始化数据文件

		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil { //递归创建目录 os.ModePerm 给予目录读写执行的权限
			return nil, err
		}
	}

	// 判断当前数据目录是否正在使用
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName)) //初始化文件锁

	//尝试去获取这把锁
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	//如果没获取到，说明有进程在使用这把锁，返回错误
	if !hold {
		return nil, ErrFilelockIsInUse
	}

	//如果序列号文件存在但是对应的文件为空 isNewInitial 也应该为 true
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isNewInitial = true
	}

	//初始化 DB 实例的结构体，对其数据结构进行初始化
	db := &DB{
		// 注意使用了引用的数据结构都需要 new 或者 make 一个空间
		option:       options,
		rwmu:         new(sync.RWMutex),
		oldFiles:     make(map[uint32]*data.DataFile),
		index:        index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites), //用户自己选择索引类型（Btree ART）
		isNewInitial: isNewInitial,
		fileLock:     fileLock,
	}

	// 首先加载 merge 的数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 然后加载对应的数据文件
	if err := db.loadDataFiles(); err != nil {
		return nil, err
	}

	//如果使用的是B+树索引类型，不需要加载索引了
	if options.IndexType != BPlusTree {
		//首先从 hint文件中加载索引
		if err := db.loadIndexFromHintFiles(); err != nil {
			return nil, err
		}

		// 然后从数据文件中加载索引的方法
		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}

	//如果是B+树类型，打开当前事务序列号的文件，取出事务序列号
	if options.IndexType == BPlusTree {
		//加载事务序列号
		if err := db.loadSeqNum(); err != nil {
			return nil, err
		}

		//直接将偏移量设置为文件的大小
		if db.activeFile != nil {
			size, err := db.activeFile.IOManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.Offsetnow = size
		}
	}

	//重置 IO 类型为标准IO类型
	if db.option.MMapAtStartup { //只用作启动加速
		if err := db.resetIOType(); err != nil {
			return nil, err
		}
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
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeqNum(key, nonTransactionSeqNum),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//调用 appendLogRecord,追加写入当前的活跃文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//拿到索引信息之后，更新内存索引
	if oldPos := db.index.Put(key, pos); oldPos != nil {
		//递增size
		db.reclaimSize += int64(oldPos.Size)
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

// Close 关闭数据库,只需要关闭当前的活跃文件即可
func (db *DB) Close() error {
	//关闭文件锁
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory,%#v", err))
		}
	}()

	//为空直接返回
	if db.activeFile == nil {
		return nil
	}

	//加锁，保证没有其他并发操作干扰到数据库的状态
	db.rwmu.Lock()
	defer db.rwmu.Unlock()

	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	//B+树拿不到事务的序列号，因此我们需要将事务序列号提前保存起来
	seqNumFile, err := data.OpenSeqNUmFile(db.option.DirPath)
	if err != nil {
		return err
	}
	//保存一条关于当前序列号的记录
	record := &data.LogRecord{
		Key:   []byte(seqNumKey),
		Value: []byte(strconv.FormatUint(db.seqNum, 10)), //当前最新的事务序列号(转为成字符串)
	}

	//直接编码后写入
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNumFile.Write(encRecord); err != nil {
		return err
	}

	//持久化
	if err := seqNumFile.Sync(); err != nil {
		return err
	}

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

// Stat 返回数据库相关信息
func (db *DB) Stat() *Stat {
	db.rwmu.RLock()
	defer db.rwmu.RUnlock()

	var dataFiles = uint(len(db.oldFiles))
	if db.activeFile != nil {
		dataFiles += 1
	}

	dirSize, err := utils.DirSize(db.option.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size,err:%#v", err))
	}

	return &Stat{
		KeyNum:      uint(db.index.Size()),
		DataFileNum: dataFiles,
		reclaimSize: db.reclaimSize,
		DiskSize:    dirSize, //待会再来写
	}

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
	defer it.Close() //关闭迭代器，因为读写事务之间是互斥的，否则会阻塞

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
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//类型判断，判断该文件是否已经被删除
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}
	//返回实际的数据
	return logRecord.Value, nil
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
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	//将删除这个标记也标记为删除
	db.reclaimSize += int64(pos.Size)

	// 在对应的内存索引中删除
	oldPos, ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	if oldPos != nil {
		db.reclaimSize += int64(oldPos.Size)
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

	//对已经写的数据字段进行递增
	db.bytesWrite += uint(size)

	//根据用户配置来决定是否需要持久化
	var isNeedSync = db.option.SyncWrites

	if !isNeedSync && db.option.BytesPerSync > 0 && db.bytesWrite >= db.option.BytesPerSync {
		//走到这里代表达到需要持久化的阈值了
		isNeedSync = true
	}

	if isNeedSync {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// 清空累计值
		if db.bytesWrite > 0 {
			db.bytesWrite = 0
		}
	}

	//构造内存索引信息，返回去上一层
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileID,
		Offset: writeOff,
		Size:   uint32(size),
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
	datafile, err := data.OpenDataFile(db.option.DirPath, defaultFileID, fio.StandardFIO)
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
		IOType := fio.StandardFIO
		if db.option.MMapAtStartup {
			IOType = fio.MemoryMap
		}

		datafile, err := data.OpenDataFile(db.option.DirPath, uint32(fid), IOType)
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
func (db *DB) loadIndexFromDataFiles() error {
	// 遍历文件中的所有记录，并加载到内存的索引中去
	// 注意：! ! map无序，想要从小到大通过fileID来添加内存索引，需要复用loadDataFiles中有序的fileIDs

	//为空直接返回
	if len(db.fileIDs) == 0 {
		return nil
	}

	//判断是否在hint文件中已经加载过的初始化变量
	hasMerged, nonMergeFileID := false, uint32(0)

	//尝试拿到了merge完成的标识
	mergeFileName := filepath.Join(db.option.DirPath, data.MergeFinishedFilename)

	//如果拿到了标识
	if _, err := os.Stat(mergeFileName); err == nil {
		//取出没有参与merge的ID
		fileID, err := db.getNonMergeFileID(db.option.DirPath)
		if err != nil {
			return err
		}
		hasMerged = true
		nonMergeFileID = fileID
	}

	//定义更新内存索引的方法
	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var oldPos *data.LogRecordPos
		//这个索引可能被删除，查看是否有墓碑值,有的话直接删除
		if typ == data.LogRecordDeleted {
			oldPos, _ = db.index.Delete(key)

			//加上墓碑值的大小
			db.reclaimSize += int64(pos.Size)

		} else {
			//正常的话就加入内存索引
			oldPos = db.index.Put(key, pos)
		}
		if oldPos != nil {
			db.reclaimSize += int64(oldPos.Size)
		}
	}

	// 缓存我们事务的数据，等待一整批事务都完成之后再更新索引
	transactionRecords := make(map[uint64][]*data.TransactionRecord) //map[序列号]

	// 序列号从零开始
	var currentSeqNum = nonTransactionSeqNum

	// 遍历所有的文件ID,取出所有文件中的内容
	for i, fid := range db.fileIDs {
		//类型转换，方便处理活跃文件和旧文件
		var fileID = uint32(fid)

		//如果发现数据文件的ID小于merge文件中的ID，那么直接跳过
		if hasMerged && fileID < nonMergeFileID {
			continue
		}

		var dataFile *data.DataFile

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
			logRecordPos := data.LogRecordPos{Fid: fileID, Offset: offset, Size: uint32(size)}

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

	// merge转化率必须要满足条件
	if options.DataFileMergeRatio < 0 || options.DataFileMergeRatio > 1 {
		return ErrInvalidMergeRatio
	}

	return nil
}

// loadReqNum加载事务序列号
func (db *DB) loadSeqNum() error {
	//拿到文件名
	fileName := filepath.Join(db.option.DirPath, data.SeqNumFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}

	//存在的话，打开这个文件
	seqNumFile, err := data.OpenSeqNUmFile(db.option.DirPath)
	if err != nil {
		return err
	}

	//取出数据
	record, _, err := seqNumFile.ReadLogRecord(0)

	//解析，拿到最新的事务序列号
	seqNum, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNum = seqNum
	//一定存在保存事务序列号文件，设置为true
	db.seqNumFileExists = true

	return os.Remove(fileName)
}

// resetIOType 将数据文件的 IO 类型设置为标准的文件IO类型（golang自带的）
func (db *DB) resetIOType() error {
	//当前活跃文件为空直接返回
	if db.activeFile == nil {
		return nil
	}

	//设置当前活跃文件
	if err := db.activeFile.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
		return err
	}

	//遍历设置旧的数据文件
	for _, dataFile := range db.oldFiles {
		if err := dataFile.SetIOManager(db.option.DirPath, fio.StandardFIO); err != nil {
			return err
		}
	}
	return nil
}
