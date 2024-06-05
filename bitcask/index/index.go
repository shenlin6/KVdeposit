package index

import (
	"bytes"

	"bitcask.go/data"
	"github.com/google/btree"
)

// 定义了一个索引的抽象接口，放入一些数据结构（后续可添加）
type Indexer interface {
	// Put 向索引中存储 key 对应数据的位置
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos

	// Get 根据 key 取出对应索引的位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据 key 删除对应索引的位置信息
	Delete(key []byte) (*data.LogRecordPos, bool)

	// 返回迭代器的
	Iterator(reverse bool) Iterator

	// 返回索引中的数据量（Key值）
	Size() int

	//关闭索引
	Close() error
}

// 定义索引类型的枚举
type IndexType = int8

const (
	// Btree 索引类型
	BTRee IndexType = iota + 1

	// ART 自适应基数数索引类型
	ART

	// B+树 索引类型
	BpTree
)

// NewIndexer 初始化索引接口实例
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case BTRee:
		return NewBtree()
	case ART:
		return NewART()
	case BpTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index data type") //不支持这种索引结构
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

// Less 判断key值，传入 bi 的 key 大于已有的 ai 的 key 保证文件读写顺序
func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// Iterator 抽象通用索引迭代器的接口
type Iterator interface {
	// 获取当前遍历位置的 Key 数据
	Key() []byte

	// 当前遍历位置的 Value 数据
	Value() *data.LogRecordPos

	// 跳转到下一个 key
	Next()

	// 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
	Seek(key []byte)

	// 重新回到迭代器的起点，即第一个数据
	Rewind()

	// 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
	Valid() bool

	// 关闭迭代器，释放相应资源
	Close()
}
