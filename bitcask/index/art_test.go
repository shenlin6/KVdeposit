package index

import (
	"testing"

	"bitcask.go/data"
	"github.com/stretchr/testify/assert"
)

// ok
func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})

	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 12})

}

// ok
func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
}

// ok
func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res1 := art.Delete([]byte("not exist"))
	t.Log(res1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	res2 := art.Delete([]byte("key-1"))
	t.Log(res2)
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
	
}

// ok
func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	assert.Equal(t, 2, art.Size())
}

// ok
func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 100})
	art.Put([]byte("adse"), &data.LogRecordPos{Fid: 1, Offset: 100})
	art.Put([]byte("bbde"), &data.LogRecordPos{Fid: 1, Offset: 200})
	art.Put([]byte("bade"), &data.LogRecordPos{Fid: 1, Offset: 200})

	iter := art.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
