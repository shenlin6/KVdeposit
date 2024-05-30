package bitcask

import (
	"bytes"

	"bitcask.go/index"
)

// Iterator 迭代器（面向用户）
type Iterator struct {
	indexIterator index.Iterator // 索引迭代器，取出 Key 和索引信息
	db            *DB            // 根据索引信息拿出Value
	options       IteratorOptions
}

// 初始化面向用户的 Iterator 结构体的方法（属于DB这个结构体，因为用户需要实际对数据进行操作）
func (db *DB) NewIterator(ops IteratorOptions) *Iterator {
	indexit := db.index.Iterator(ops.Reverse)
	return &Iterator{
		indexIterator: indexit,
		db:            db,
		options:       ops,
	}
}

// 提供给用户的几个 KV 操作接口

// Key 获取当前遍历位置的 Key 数据
func (it *Iterator) Key() []byte {
	return it.indexIterator.Key()
}

// Value 获取当前位置的索引信息
func (it *Iterator) Value() ([]byte, error) {
	//拿到对应的索引信息
	logRecordPos := it.indexIterator.Value() //这里的Value实际上是位置的索引信息

	//用户操作:必须加锁
	it.db.rwmu.RLock()         //读锁，阻止写操作的发生，确保在读取过程中资源不被修改
	defer it.db.rwmu.RUnlock() //释放读锁

	//处理这个索引信息返回用户真正储存的value
	v, err := it.db.getValueByPosition(logRecordPos)
	if err != nil {
		return nil, err
	}
	return v, err
}

// 跳转到下一个 key
func (it *Iterator) Next() {
	it.indexIterator.Next()
	it.skipOne()
}

// 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIterator.Seek(key)
	it.skipOne()
}

// 重新回到迭代器的起点，即第一个数据
func (it *Iterator) Rewind() {
	it.indexIterator.Rewind()
	it.skipOne()
}

// 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (it *Iterator) Valid() bool {
	return it.indexIterator.Valid()
}

// 关闭迭代器，释放相应资源
func (it *Iterator) Close() {
	it.indexIterator.Close()
}

// skipOne 判断Key是否带有用户需要用于过滤的前缀，不符合前缀的直接跳过一个索引
func (it *Iterator) skipOne() {
	prefixLen := len(it.options.Prefix)

	//如果用户没有指定 Prefix 的话，不用做处理
	if prefixLen == 0 {
		return
	}

	//循环遍历所有的索引，来查找符合用户指定前缀的文件(处理方法：死循环+跳出)
	for ; it.indexIterator.Valid(); it.indexIterator.Next() {
		key := it.indexIterator.Key()

		//如果前缀小于等于key的长度，并且这两个前缀字典顺序完全一致时，就说明找到了用户需要的文件
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}

	}
}
