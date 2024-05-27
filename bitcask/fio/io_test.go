package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewFileIDManager(t *testing.T) {
	fid, err := NewFileIDManager(filepath.Join("C:\\tmp", "dataA"))
	assert.Nil(t, err)
	assert.NotNil(t, fid)
}

func TestFilename_Write(t *testing.T) {
	fid, err := NewFileIDManager(filepath.Join("C:\\tmp", "dataA"))
	assert.Nil(t, err)
	assert.NotNil(t, fid)

	//case nil
	n, err := fid.Write([]byte("")) 
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fid.Write([]byte("abcdefg"))
	t.Log(n, err)
	n, err = fid.Write([]byte("golang gogogo")) 
	t.Log(n, err)

}
