package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unsafe"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

var (
	modkernel32            = syscall.NewLazyDLL("kernel32.dll")
	procGetDiskFreeSpaceEx = modkernel32.NewProc("GetDiskFreeSpaceExW")
)

// AvailableDiskSize 获取当前目录下的磁盘剩余可用空间大小
func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	// 获取当前目录所在的驱动器路径
	drive := wd[:1]

	// 准备参数
	lpFreeBytesAvailableToCaller := int64(0)
	lpTotalNumberOfBytes := int64(0)
	lpTotalNumberOfFreeBytes := int64(0)
	lpDirectoryName, err := syscall.UTF16PtrFromString(drive)

	if err != nil {
		return 0, err
	}

	// 调用 Windows API 函数 GetDiskFreeSpaceEx
	r, _, err := procGetDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(lpDirectoryName)),
		uintptr(unsafe.Pointer(&lpFreeBytesAvailableToCaller)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfBytes)),
		uintptr(unsafe.Pointer(&lpTotalNumberOfFreeBytes)))

	if r == 0 {
		return 0, err
	}

	// 返回剩余可用空间大小
	return uint64(lpFreeBytesAvailableToCaller), nil
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标目标不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name())
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
