package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

// ok
func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	dirSize, err := DirSize(dir)
	assert.Nil(t, err)
	assert.True(t, dirSize > 0)
}
