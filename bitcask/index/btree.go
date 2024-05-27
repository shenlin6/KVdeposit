package index

import (
	"sync"

	"bitcask.go/data"
	"github.com/google/btree"
)

// 引用了 google 的 Btree
type Btree struct {
	tree btree.BTree
	lock *sync.RWMutex //加锁，避免用户多线程访问时冲突
}

// NewBtree 初始化 Btree 索引结构
func NewBtree() *Btree {
	return &Btree{
		tree: *btree.New(32), //控制叶子节点数量（可以让用户进行选择）
		lock: new(sync.RWMutex),
	}

}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Itom{Key: key, pos: pos}

	bt.lock.Lock() //进行存储操作之前加锁
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock() //解锁
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Itom{Key: key}
	btreeItem := bt.tree.Get(it)
	//如果拿到的为空，直接返回:
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Itom).pos //转换为对应类型的数据
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Itom{Key: key}
	bt.lock.Lock() //进行存储操作之前加锁
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock() //释放
	
	return oldItem != nil //为空说明我们删除操作无效，反之成功
}
