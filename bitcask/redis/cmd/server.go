package main

import (
	"log"
	"sync"

	bitcask "bitcask.go"
	bitcask_redis "bitcask.go/redis"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380" // 避免与6379冲突

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructureType
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 redis 数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructureType(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 bitcaskserver
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructureType),
	}

	//默认一开始打开第0个数据库
	bitcaskServer.dbs[0] = redisDataStructure

	//初始化 Redis Server端服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

// listen 监听客户端连接
func (svr *BitcaskServer) listen() {
	log.Println("bitcask server is running ,ready to accept connection")
	_ = svr.server.ListenAndServe()

}

// accept 处理新的连接
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	// 如果有新的连接进来，初始化客户端
	cli := new(BitcaskClient)

	svr.mu.Lock()
	defer svr.mu.Unlock()

	cli.server = svr
	cli.db = svr.dbs[0] //默认为第0个数据库

	//放入上下文，方便取出
	conn.SetContext(cli)

	return true
}

// close 断开连接后的处理
func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	// 将所有数据库关闭掉
	for _, db := range svr.dbs {
		_ = db.Close()
	}

	// 将所有 server 端关闭
	_ = svr.server.Close()
}
