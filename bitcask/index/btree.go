package index

import (
	"bytes"
	"sort"
	"sync"

	"bitcask.go/data"
	"github.com/google/btree"
)

// 引用了 google 的 Btree
type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex //加锁，避免用户多线程访问时冲突
}

// NewBtree 初始化 Btree 索引结构
func NewBtree() *Btree {
	return &Btree{
		tree: btree.New(32), //控制叶子节点数量（可以让用户进行选择）
		lock: new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}

	bt.lock.Lock() //进行存储操作之前加锁
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock() //解锁
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	//如果拿到的为空，直接返回:
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos //转换为对应类型的数据
}

func (bt *Btree) Size() int {
	//使用Btree中的Len函数
	return bt.tree.Len()
}

func (bt *Btree) Iterator(reverse bool) Iterator {
	// 调用新建iterator的方法并返回

	//判断树是否为空
	if bt.tree == nil {
		return nil
	}

	// 加锁，避免用户多线程访问时冲突
	bt.lock.RLock()
	defer bt.lock.RUnlock() // 读解锁，允许多个 goroutine 同时进行读取操作,但进行写入操作时会阻塞其他读写操作
	return newBtreeIterator(bt.tree, reverse)
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock() //进行存储操作之前加锁
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock() //释放

	return oldItem != nil //为空说明我们删除操作无效，反之成功
}

func (bt *Btree) Close()error{
	return nil
}


// Btree 索引迭代器
type btreeIterator struct {
	curindex int     // 当前遍历到哪个位置了
	reverse  bool    // 是否反向遍历，默认为false
	values   []*Item // 存放从索引中拿出来的对应的Key和索引信息

}

// newBtreeIterator 实例化索引迭代器
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var index int // 数组的索引
	values := make([]*Item, tree.Len())

	//将所有数据存放到 savevalues 数组中
	savevalues := func(bti btree.Item) bool {
		values[index] = bti.(*Item)
		index++
		return true //表示一直向下遍历
	}

	if reverse {
		//降序遍历，Descend 中需要传入一个自定义的函数,对于每个 key 按照顺序执行自定义函数的逻辑
		tree.Descend(savevalues)
	} else {
		// 使用btree包里的方法,升序遍历数据
		tree.Ascend(savevalues)
	}

	return &btreeIterator{
		curindex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// Key 获取当前遍历位置的 Key 数据
func (bti *btreeIterator) Key() []byte {
	// 返回当前索引对应的 Key
	return bti.values[bti.curindex].key
}

// Value 当前遍历位置的 Value 数据
func (bti *btreeIterator) Value() *data.LogRecordPos {
	// 返回当前索引对应的 Value
	return bti.values[bti.curindex].pos
}

// Next 跳转到下一个 key
func (bti *btreeIterator) Next() {
	// 将指针位置向前移动一次
	bti.curindex += 1
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	// 因为我们已经排好序，查找Key的位置，使用二分查找实现
	if bti.reverse { //逆序实现
		// bti.curindex 每次都在查找到的Index处，然后从这个位置开始遍历
		bti.curindex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else { //正序则实现原理相反
		bti.curindex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (bti *btreeIterator) Rewind() {
	// 直接把 index 变为0，即可完成重新返回迭代器起点
	bti.curindex = 0
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	//判断指针是否超出数组的长度
	return bti.curindex < len(bti.values)
}

// Close 关闭迭代器，释放相应资源
func (bti *btreeIterator) Close() {
	// 将建立的临时的数组释放干净，以便于下次迭代使用
	bti.values = nil
}
