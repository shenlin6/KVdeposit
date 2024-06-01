package data

import (
	"os"
	"testing"

	"bitcask.go/fio"
	"github.com/stretchr/testify/assert"
)

// ok
func TestOpenDataFile(t *testing.T) {

	dataFileA, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileA)

	// case 打开多个文件
	dataFileB, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileB)

	// case 重复打开一个文件
	dataFileC, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileC)
}

// ok
func TestData_Write(t *testing.T) {
	dataFileA, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileA)

	err = dataFileA.Write([]byte("my name is shone"))
	assert.Nil(t, err)

	// case 追加写
	err = dataFileA.Write([]byte("i want to join lanshan"))
	assert.Nil(t, err)
}

// ok
func TestData_Close(t *testing.T) {
	dataFileA, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileA)

	err = dataFileA.Write([]byte("my name is shone"))
	assert.Nil(t, err)

	err = dataFileA.Close()
	assert.Nil(t, err)
}

// ok
func TestData_Sync(t *testing.T) {
	dataFileA, err := OpenDataFile(os.TempDir(), 2, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFileA)

	err = dataFileA.Write([]byte("i love golang"))
	assert.Nil(t, err)

	err = dataFileA.Sync()
	assert.Nil(t, err)
}
