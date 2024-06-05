package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	bitcask "bitcask.go"
)

var db *bitcask.DB

func init() {
	// 打开存储引擎实例来调用HTTP

	var err error
	options := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-http")
	options.DirPath = dir
	db, err = bitcask.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open a database,err:%#v\n", err))
	}
}

func main() {
	//注册处理方法

	// PUT
	http.HandleFunc("/bitcask/put", handlePut)
	// GET
	http.HandleFunc("/bitcask/get", handleGet)
	// DELETE
	http.HandleFunc("/bitcask/delete", handleDelete)
	// LISTKEYS
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	// STAT
	http.HandleFunc("/bitcask/listkeys/statinfo", handleStat)

	// 启动 HTTP 服务
	_ = http.ListenAndServe("localhost:8080", nil)

	//fmt.Printf("hello")
}

// handlePut HTTP PUT 方法
func handlePut(writer http.ResponseWriter, request *http.Request) {
	//如果不是PUT类型直接返回错误，不执行请求
	if request.Method != http.MethodPut {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	//对用户传递的参数进行解析,储存到变量data中
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}

	// 遍历处理解析到的参数
	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			//这里出错的话就是程序内部出错了，对用户显示内部错误
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to put value in database,err:%#v\n", err)
			return
		}
	}
}

// HTTP GET 方法
func handleGet(writer http.ResponseWriter, request *http.Request) {
	//如果不是GET类型直接返回错误，不执行请求
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	//从用户的路劲参数中拿到Key，查询对应的Value
	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))

	//如果 err 存在并且这个 err 不是对应Key为空，则说明程序内部出了问题
	if err != nil && err != bitcask.ErrKeyIsEmpty {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value from database,err:%#v\n", err)
		return
	}

	//将对应的 value 用 json 格式返回给用户
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

// HTTP Delete 方法
func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	//从用户的路劲参数中拿到Key，查询对应的Value
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))

	//如果 err 存在并且这个 err 不是对应Key为空，则说明程序内部出了问题
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete from database,err:%#v\n", err)
		return
	}

	//将对应的 value 用 json 格式返回给用户
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

// HTTP LISTKEYS
func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	//调用存储引擎
	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	//转化为字符串
	var res []string
	for _, key := range keys {
		res = append(res, string(key))
	}

	_ = json.NewEncoder(writer).Encode(res)
}

// handleStat 获取数据库的统计信息（内存，数据量什么的）
func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	statinfo := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(statinfo)
}





