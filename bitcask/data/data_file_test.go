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

// ok
func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 1000, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	// 只有一条 LogRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)
	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)
	t.Log(readSize1)

	// 多条 LogRecord，从不同的位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := dataFile.ReadLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, readRec2)
	assert.Equal(t, size2, readSize2)

	// 被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, readRec3)
	assert.Equal(t, size3, readSize3)
}
