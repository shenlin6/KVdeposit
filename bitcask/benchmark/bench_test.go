package benchmark

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"bitcask.go"
	"bitcask.go/utils"
	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

// init 初始化存储引擎
func init() {
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-bench")
	options.DirPath = dir

	var err error
	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}
}

// ok
func Benchmark_Put(b *testing.B) {
	// 重置计时器，输出更精确的结果
	b.ResetTimer()
	//打印每个方法内存分布情况
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(100))
		assert.Nil(b, err)
	}

}

// ok
func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
	//随机生成数据
	// 使用当前时间的纳秒数作为种子来初始化随机数生成器
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	//重置计时器
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(r.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

// ok
func Benchmark_ListKeys(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	//重置计时器
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.ListKeys()
	}
}

// ok
func Benchmark_Delete(b *testing.B) {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(r.Int()))
		assert.Nil(b, err)
	}
}
