package fio

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIDManager(t *testing.T) {
	fid, err := NewFileIOManager(filepath.Join("example", "dataA"))
	assert.Nil(t, err)
	assert.NotNil(t, fid)
}

func TestFileIO_Write(t *testing.T) {

	path := filepath.Join("C:\\tmp", "dataA")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	// case nil
	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)
	// case 带有空格
	n, err = fio.Write([]byte("golang gogogo"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)
	// case 无空格
	n, err = fio.Write([]byte("shone"))
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "dataA")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)

	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key1"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key2"))
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n, err := fio.Read(b1, 0)
	assert.Equal(t, 10, n)
	assert.Equal(t, []byte("key1"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key2"), b2)
}
