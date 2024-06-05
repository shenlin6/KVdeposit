package index

import (
	"bytes"
	"sort"
	"sync"

	"bitcask.go/data"
	goart "github.com/plar/go-adaptive-radix-tree"
)

//用自适应奇数树作为索引类型
//引用了这个库:  https://github.com/plar/go-adaptive-radix-tree``

type AdaptiveRadixTree struct {
	tree goart.Tree

	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

// Put 向索引中存储 key 对应数据的位置
func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	//直接调用art的方法
	art.lock.Lock()

	oldValue, _ := art.tree.Insert(key, pos)

	art.lock.Unlock()

	if oldValue == nil {
		return nil
	}

	return oldValue.(*data.LogRecordPos)
}

// Get 根据 key 取出对应索引的位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)

	//没有找到就直接返回空
	if !found {
		return nil
	}

	//强转为我们需要的类型
	return value.(*data.LogRecordPos)
}

// Delete 根据 key 删除对应索引的位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()

	oldValue, deleted := art.tree.Delete(key)

	art.lock.Unlock()

	if oldValue == nil {
		return nil, false
	}

	//返回旧数据和是否被删除
	return oldValue.(*data.LogRecordPos), deleted
}

// Size 返回索引中的数据量（Key值）
func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	size := art.tree.Size()
	art.lock.RUnlock()

	return size
}

// Iterator 返回迭代器的方法
func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()

	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// ART 索引迭代器
type artIterator struct {
	curindex int     // 当前遍历到哪个位置了
	reverse  bool    // 是否反向遍历，默认为false
	values   []*Item // 存放从索引中拿出来的对应的Key和索引信息

}

// newArtIterator 实例化索引迭代器
func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var index int // 数组的索引

	if reverse {
		index = tree.Size() - 1 //注意0
	}

	values := make([]*Item, tree.Size())
	// Node 存储了Key和Value
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[index] = item
		if reverse { //如果反向的话index递减
			index--
		} else {
			index++
		}
		return true
	}

	//如果是反向的情况，我们从后往前存放数据（art树没有直接提供反向存储数据的方法）

	//完整遍历数据
	tree.ForEach(saveValues)

	return &artIterator{
		curindex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// Key 获取当前遍历位置的 Key 数据
func (ai *artIterator) Key() []byte {
	// 返回当前索引对应的 Key
	return ai.values[ai.curindex].key
}

// Value 当前遍历位置的 Value 数据
func (ai *artIterator) Value() *data.LogRecordPos {
	// 返回当前索引对应的 Value
	return ai.values[ai.curindex].pos
}

// Next 跳转到下一个 key
func (ai *artIterator) Next() {
	// 将指针位置向前移动一次
	ai.curindex += 1
}

// Seek 根据传入的 key 查找到第一个大于（或小于）等于的目标 key，根据从这个 key 开始遍历
func (ai *artIterator) Seek(key []byte) {
	// 因为我们已经排好序，查找Key的位置，使用二分查找实现
	if ai.reverse { //逆序实现
		// ai.curindex 每次都在查找到的Index处，然后从这个位置开始遍历
		ai.curindex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else { //正序则实现原理相反
		ai.curindex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据
func (ai *artIterator) Rewind() {
	// 直接把 index 变为0，即可完成重新返回迭代器起点
	ai.curindex = 0
}

// Valid 是否有效，即是否已经遍历完了所有的 key，用于退出遍历
func (ai *artIterator) Valid() bool {
	//判断指针是否超出数组的长度
	return ai.curindex < len(ai.values)
}

// Close 关闭迭代器，释放相应资源
func (ai *artIterator) Close() {
	// 将建立的临时的数组释放干净，以便于下次迭代使用
	ai.values = nil
}
