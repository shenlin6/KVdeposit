package bitcask

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

	"bitcask.go/data"
)

// nonTransactionSeqNum 标志是否是事务操作
const nonTransactionSeqNum uint64 = 0

// txnFinKey 事务的完成键
var txnFinKey = []byte("Transaction Finished !")

// WriteBatch 原子批量写入数据，保证原子性
type WriteBatch struct {
	mu            *sync.Mutex //互斥锁
	db            *DB
	options       WriteBatchOptions          // 用户自己的配置项
	pendingWrites map[string]*data.LogRecord // 缓存用户写入，暂时不提交，保证并发安全
}

// NewWriteBatch 初始化原子写实例
func (db *DB) NewWriteBatch(options WriteBatchOptions) *WriteBatch {
	//如果是B+树索引类型的话并且事务序列号文件不存在并且不是第一次初始化序列号文件的话，需要禁用 WriteBatch 功能
	if db.option.IndexType == BPlusTree && !db.seqNumFileExists && !db.isNewInitial {
		panic("write batch is banned because no file exists")
	}

	return &WriteBatch{
		mu:            new(sync.Mutex),
		db:            db,
		options:       options,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put 批量操作的写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	// 校验Key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()

	//将用户写入的数据存在LogRecord中缓存起来
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
	} //注意默认Type值（正常情况）
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	// 校验Key
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 如果用户要删除的数据不存在(或在缓存中)直接删除缓存信息并返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存这个LogRecord
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDeleted, //注意修改Type值（删除的情况）
	}
	wb.pendingWrites[string(key)] = logRecord

	return nil
}

// Commit 将批量写入的缓存数据全部写入磁盘中，并且更新索引
func (wb *WriteBatch) Commit() error {
	//加锁
	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 如果缓存为空，直接返回
	if len(wb.pendingWrites) == 0 {
		return nil
	}

	// 如果缓存的批量数据超过的我们定义的最大量，返回一个错误
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	// 校验通过，开始实际写入数据

	//对数据库也加锁，保证事务的串行化
	wb.db.rwmu.Lock()
	defer wb.db.rwmu.Unlock()

	// 1. 获取事务的序列号
	// 进行原子性的增加操作,严格递增的同时保证并发安全
	seqNum := atomic.AddUint64(&wb.db.seqNum, 1)

	// 2. 将所有的缓存数据写进数据文件当中
	positions := make(map[string]*data.LogRecordPos)
	for _, logRecord := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			//需要加上我们的序列号
			Key:   logRecordKeyWithSeqNum(logRecord.Key, seqNum),
			Value: logRecord.Value,
			Type:  logRecord.Type,
		})
		if err != nil {
			return err
		}

		// 拿到了索引位置的信息，我们不提交，等所有事务都完成后再写进磁盘
		positions[string(logRecord.Key)] = logRecordPos
	}

	// 我们需要写一条事务是否完成提交的标识，读取的时候需要找到这条标识，标识我们事务提交成功
	finishRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNum(txnFinKey, seqNum),
		Type: data.LogRecordTxnFinished,
	}

	// 追加一条事务完成键
	_, err := wb.db.appendLogRecord(finishRecord)
	if err != nil {
		return err
	}

	//走到这里表示所有的数据都已经写到数据文件中了
	//根据用户配置进行持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		//对当前的活跃文件进行持久化
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		//如果 Type 是正常类型的话就更新内存索引信息
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}

		//如果 Type 是被删除的数据类型则从对应的索引中删除
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	//走到这里说明 Commit 逻辑处理已经完成，需要清理缓存数据，以便于下次 Commit
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeqNum 对Key 和 seq进行编码处理
func logRecordKeyWithSeqNum(key []byte, seqNum uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	//将序列号变为可变长度的切片,拿到返回的字节数
	n := binary.PutUvarint(seq[:], seqNum)

	//把序列号和Key整合到一个切片中，序列号放在前面，后面跟着的是用户实际的Key
	encodeKey := make([]byte, n+len(key))
	copy(encodeKey[:n], seq[:n])
	copy(encodeKey[n:], key)

	return encodeKey
}

// parseLogRecordKey 解析LogRecord的Key，获取实际的Key和序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	// 将无符号整数反序列化为对应的整数值
	seqNum, n := binary.Uvarint(key) //拿到最后一个seqNum的位置，后面的所有都是实际的Key了
	realKey := key[n:]

	return realKey, seqNum
}
