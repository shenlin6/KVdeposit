package index

import (
	"os"
	"path/filepath"
	"testing"

	"bitcask.go/data"
	"github.com/stretchr/testify/assert"
)

// ok
func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 111, Offset: 666})

	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 222, Offset: 666})

	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 333, Offset: 666})

}

// ok
func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9999, Offset: 1234})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
}

// ok
func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res1 := tree.Delete([]byte("not exist"))
	t.Log(res1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Delete([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 456, Offset: 3212})
	pos2 := tree.Delete([]byte("aac"))
	assert.NotNil(t, pos2)

}

// ok
func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	t.Log(tree.Size())

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

	t.Log(tree.Size())
}

// ok
func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("caac"), &data.LogRecordPos{Fid: 1, Offset: 10})
	tree.Put([]byte("bbca"), &data.LogRecordPos{Fid: 2, Offset: 20})
	tree.Put([]byte("acce"), &data.LogRecordPos{Fid: 3, Offset: 30})
	tree.Put([]byte("ccec"), &data.LogRecordPos{Fid: 4, Offset: 40})
	tree.Put([]byte("bbba"), &data.LogRecordPos{Fid: 4, Offset: 50})

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
