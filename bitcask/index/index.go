package index

import (
	"bytes"

	"bitcask.go/data"
	"github.com/google/btree"
)

// 定义了一个索引的抽象接口，放入一些数据结构（后续可添加）
type Indexer interface {
	// Put 向索引中存储 key 对应数据的位置
	Put(key []byte, pos *data.LogRecordPos) bool
   // Get 根据 key 取出对应索引的位置信息
	Get(key []byte) *data.LogRecordPos
	// Delete 根据 key 删除对应索引的位置信息
	Delete(key []byte) bool
}

type Itom struct {
	Key []byte
	pos *data.LogRecordPos
}

// Less 判断key值，传入 bi 的 key 大于已有的 ai 的 key 保证文件读写顺序
func (ai *Itom) Less(bi btree.Item) bool {
	return bytes.Compare(ai.Key, bi.(*Itom).Key) == -1
}
