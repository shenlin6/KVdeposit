package bitcask

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"

	"bitcask.go/data"
	"bitcask.go/utils"
)

const (
	mergeFileName    = "-merge"
	mergeFinishedKey = "merge.finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.rwmu.Lock()
	if db.isMerging {
		//勿忘解锁后返回所悟
		db.rwmu.Unlock()
		return ErrIsMergeNow
	}

	//查看当前无效数据是否达到用户设置的merge ratio 阈值
	totalSize, err := utils.DirSize(db.option.DirPath)
	if err != nil {
		//注意，要解锁
		db.rwmu.Unlock()
		return err
	}

	//算出比例,如果还没达到用户设置的阈值就直接返回
	if float32(db.reclaimSize)/float32(totalSize) < db.option.DataFileMergeRatio {
		db.rwmu.Unlock()
		return ErrUnderMergeRatio
	}

	//查看剩余空间容量是否可以装下Merge后的数据量
	availableDiskSize, err := utils.AvailableDiskSize()
	if err != nil {
		db.rwmu.Unlock()
		return err
	}
	//如果超过了磁盘的容量直接返回错误
	if uint64(totalSize-db.reclaimSize) >= availableDiskSize {
		db.rwmu.Unlock()
		return ErrNotEnoughSpaceToMerge
	}

	//如果没有在Merge,我们开始进行Merge操作
	db.isMerging = true
	defer func() {
		db.isMerging = false //最后执行完毕之后重新设置为false
	}()

	//对当前的活跃文件持久化处理
	if err := db.activeFile.Sync(); err != nil {
		db.rwmu.Unlock()
		return nil
	}

	//将当前的活跃文件转化为旧的文件
	db.oldFiles[db.activeFile.FileID] = db.activeFile

	// 生成一个新的活跃文件
	if err := db.setActiveFile(); err != nil {
		db.rwmu.Unlock()
		return nil
	}
	// 这个文件没有参与Merge操作
	nonMergeFileId := db.activeFile.FileID

	//取出所有需要Merge的文件，并可以释放锁了
	var mergeFiles []*data.DataFile

	for _, file := range db.oldFiles {

		mergeFiles = append(mergeFiles, file)

	}
	db.rwmu.Unlock() //释放锁，下次Merge的时候可以拿到锁

	//将Merge的文件从小到大进行排序，依次Merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileID < mergeFiles[j].FileID
	})

	mergePath := db.getMergePath()
	// 如果merge文件还存在，说明之前发生过Merge,不需要再Merge一遍，直接删除
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//新建一个Merge目录，存起来(os.ModePerm 设置权限为完全的读、写、执行权限)
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	mergeOptions := db.option
	// 改变merge的路径
	mergeOptions.DirPath = mergePath
	// 这里不需要Sync，每次打开都Sync会降低很多性能
	mergeOptions.SyncWrites = false

	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	// 打开hint文件储存索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历所有需要Merge的文件，重写有效数据
	for _, dataFile := range mergeFiles {
		//从零开始遍历
		var offset int64 = 0
		for {

			logRecord, logRecordSize, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				//如果读到最后一个文件则退出
				if err == io.EOF {
					break
				}
				return err
			}

			//解析拿到的Key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			//获取内存索引信息
			logRecordPos := db.index.Get(realKey)

			//和内存中的所有进行比较判断，如果是有效的数据则重写
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileID && logRecordPos.Offset == offset {
				//写进临时目录当中
				logRecord.Key = logRecordKeyWithSeqNum(realKey, nonTransactionSeqNum)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 将当前位置的索引写入hint文件,创建一条新的LogRecord方法，但只储存索引和原始的Key
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			//修改偏移量,保证偏移量在最新的位置
			offset += logRecordSize
		}
	}

	// 所有文件都重写完之后，才开始持久化操作（对最新的hintFile）
	if err := hintFile.Sync(); err != nil {
		return err
	}
	if err := mergeDB.Sync(); err != nil {
		return err
	}

	//在末尾添加一个标识Merge完成的标识
	mergeFinishdeFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinishedRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	//比最后这个merge文件小的，代表都经过了merge处理
	encRecord, _ := data.EncodeLogRecord(mergeFinishedRecord)
	if err := mergeFinishdeFile.Write(encRecord); err != nil {
		return err
	}

	//写完之后，对最后一个标识文件进行持久化
	if err := mergeFinishdeFile.Sync(); err != nil {
		return err
	}

	return nil
}

// 获取merge文件目录的函数
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.option.DirPath)) // path.Clean:去掉末尾的 /
	base := path.Base(db.option.DirPath)
	return filepath.Join(dir, base+mergeFileName)
}

// loadMergeFiles 加载 merge 数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()

	//不存在直接返回
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		//将所有Merge目录删除
		_ = os.RemoveAll(mergePath)
	}()

	//将整个merge目录读取出来
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找末尾是否有merge完成的标识
	var mergeFinished bool
	//保存所有Merge文件的名称
	var mergeFileNames []string

	for _, entry := range dirEntries {
		//如果找到了这个标识，说明所有Merge都处理完了
		if entry.Name() == data.MergeFinishedFilename {
			mergeFinished = true
		}
		//如果遍历到了事务序列号的文件，直接跳过
		if entry.Name() == data.SeqNumFileName {
			continue
		}
		// 如果是文件锁的目录就直接跳过
		if entry.Name() == fileLockName {
			continue
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	//没有Merge完成标识，直接返回
	if !mergeFinished {
		return nil
	}

	//merge完成之后，我们需要将旧的文件删除，用新的merge文件替代
	//但是我们需要取到最后一个没有merge的文件（可以看作活跃文件）否则会导致数据不完整
	nonMergeFileID, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}

	// 开始删除旧的对应数据文件，即FIleID比nonMergeFileID小的ID(递增原则)
	var fileID uint32 = 0
	for ; fileID < nonMergeFileID; fileID++ {
		//先获取对应文件的名称
		fileName := data.GetDataFileName(db.option.DirPath, fileID)
		//如果数据存在就删除掉
		if _, err := os.Stat(fileName); err != nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	// 将新的数据文件移动过来
	for _, fileName := range mergeFileNames {
		//更改对应的目录

		//源目录
		srcPath := filepath.Join(mergePath, fileName)
		//目标地址
		desPath := filepath.Join(db.option.DirPath, fileName)

		//更改名称即可
		if err := os.Rename(srcPath, desPath); err != nil {
			return err
		}
	}

	return nil
}

// getNonMergeFileID 获取最新一个没有参与Merge操作的文件ID
func (db *DB) getNonMergeFileID(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}

	record, _, err := mergeFinishedFile.ReadLogRecord(0) //因为之后一条数据，所有偏移量为0
	if err != nil {
		return 0, err
	}

	nonMergeFIleID, err := strconv.Atoi(string(record.Value)) //将字符串转为int 类型
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFIleID), nil
}

// 从hint文件中加载所有
func (db *DB) loadIndexFromHintFiles() error {
	// 判断 HInt 文件是否存在
	hintFileName := filepath.Join(db.option.DirPath, data.HintFilename)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	//打开Hint文件
	hintFile, err := data.OpenHintFile(db.option.DirPath)
	if err != nil {
		return err
	}

	//读取文件中的索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				//如果文件读到头了直接跳出循环
				break
			}
			//其他情况返回错误
			return err
		}

		//解码，拿到实际的索引信息
		logRecordPos := data.DecodeLogRecordPos(logRecord.Value)

		//存放到索引当中
		db.index.Put(logRecord.Key, logRecordPos)

		//别忘修改偏移量
		offset += size
	}
	return nil
}
