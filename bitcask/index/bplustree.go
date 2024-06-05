package index

import (
	"path/filepath"

	"bitcask.go/data"
	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

//B+树索引类型
// 引用： go.etcd.io/bbolt

type BPlusTree struct {
	tree *bbolt.DB //内部支持并发访问
}

// NewBPlusTree 初始化B+树
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	//传入配置项，根据实际情况调用
	options := bbolt.DefaultOptions
	options.NoSync = !syncWrites //取反操作，保持一致

	//打开实例
	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, options) // 0644:（用户读写，其他用户只读）
	if err != nil {
		panic("failed to open bptree")
	}

	// 创建对应的bucket，可以看作一个单独的事务
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		//创建一个分区
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{
		tree: bptree,
	}
}

// Put 向索引中存储 key 对应数据的位置
func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	//读写之前先把旧数据取出来
	var oldValue []byte

	//拿到bucket去读写数据
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error { //bpt.tree.Update 自动开启了一个事务
		bucket := tx.Bucket(indexBucketName)

		oldValue = bucket.Get(key)

		return bucket.Put(key, data.EncodeLogRecordPos(pos)) //需要编码索引信息后传入
	}); err != nil {
		panic("failed to put value in bptree")
	}

	if len(oldValue) == 0 {
		return nil
	}

	//返回解码后的旧数据
	return data.DecodeLogRecordPos(oldValue)
}

// Get 根据 key 取出对应索引的位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	//利用B+树包装的方法读数据
	if err := bpt.tree.View(func(tx *bbolt.Tx) error { //只读或者删除数据
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value) //读数据的时候解码
		}

		return nil
	}); err != nil {
		panic("failed to get value in bptree")
	}

	return pos
}

// Delete 根据 key 删除对应索引的位置信息
func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte

	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		if oldValue = bucket.Get(key); len(oldValue) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value in bptree")
	}

	if len(oldValue) == 0 {
		return nil, false
	}

	return data.DecodeLogRecordPos(oldValue), true
}

// Size 返回索引中的数据量（Key值）
func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error { //只读或者删除数据
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN //key 的数量
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}

	return size
}

func (bpt *BPlusTree) Close() error {
	return bpt.Close()
}

// Iterator 返回迭代器
func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBptreeIterator(bpt.tree, reverse)
}

// bptreeIterator B+树迭代器
type bptreeIterator struct {
	tx      *bbolt.Tx
	cursor  *bbolt.Cursor
	reverse bool

	cursorKey   []byte
	cursorValue []byte
}

// newBptreeIterator 实例化索引迭代器
func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	//手动开启一个事务
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}

	//手动调用Rewind，重新回到迭代器的第一个位置
	bpi.Rewind()
	return bpi
}

// 迭代器的几个方法

// 获取当前遍历位置的 Key 数据
func (bpi *bptreeIterator) Key() []byte {
	return bpi.cursorKey
}

// 当前遍历位置的 Value 数据
func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	//解码后返回当前的数据
	return data.DecodeLogRecordPos(bpi.cursorValue)

}

// 跳转到下一个 key
func (bpi *bptreeIterator) Next() {
	//反向迭代反向下一个
	if bpi.reverse {
		//将对应的K和V存储起来
		bpi.cursorKey, bpi.cursorValue = bpi.cursor.Prev()
	} else {
		bpi.cursorKey, bpi.cursorValue = bpi.cursor.Next()
	}

}

// 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bpi *bptreeIterator) Seek(key []byte) {

	bpi.cursorKey, bpi.cursorValue = bpi.cursor.Seek(key)

}

// 重新回到迭代器的起点，即第一个数据
func (bpi *bptreeIterator) Rewind() {
	//反向迭代则返回最后一个
	if bpi.reverse {
		//将对应的K和V存储起来
		bpi.cursorKey, bpi.cursorValue = bpi.cursor.Last()
	} else {
		bpi.cursorKey, bpi.cursorValue = bpi.cursor.First()
	}
}

// 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bpi *bptreeIterator) Valid() bool {
	//看当前的Key是否为空就可以了,为空则说明遍历完了
	return len(bpi.cursorKey) != 0

}

// 关闭迭代器，释放相应资源
func (bpi *bptreeIterator) Close() {
	// 只读的事务需要回滚
	_ = bpi.tx.Rollback()
}
