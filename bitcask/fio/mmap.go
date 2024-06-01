package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

//我们只需要通过内存映射来读取数据就可以了

// MMap 内存映射
type MMap struct {
	readerAt *mmap.ReaderAt
}

// 初始化MMap
func NewMMapIOManager(fileName string) (*MMap, error) {
	//如果文件不存在，先创建一个
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	// 使用包内部自带的方法
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &MMap{
		readerAt: readerAt,
	}, nil
}

// Read 从对应位置读取数据
func (mmp *MMap) Read(b []byte, offset int64) (int, error) {
	return mmp.readerAt.ReadAt(b, offset) //读取内容，并将数据缓存到 b 中
}

// Write 写入字节级别文件中
func (mmp *MMap) Write([]byte) (int, error) {
	// 不用这个来写数据
	panic("not implemented")
}

// Sync 持久化数据
func (mmp *MMap) Sync() error {
	//不用内存索引来持久化
	panic("not implemented")
}

// Close 关闭文件
func (mmp *MMap) Close() error {
	return mmp.readerAt.Close()
}

// Size 获取剩余文件大小
func (mmp *MMap) Size() (int64, error) {
	return int64(mmp.readerAt.Len()), nil
}
