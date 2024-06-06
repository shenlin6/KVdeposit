package main

import (
	"fmt"
	"strings"

	"bitcask.go"
	bitcask_redis "bitcask.go/redis"
	"bitcask.go/utils"
	"github.com/tidwall/redcon"
)

// 参数错误的函数
func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("invalid number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

// 支持哪些操作类型（map 的 value 为处理函数）
var supportedCommands = map[string]cmdHandler{
	"set":       set,
	"get":       get,
	"hset":      hset,
	"sadd":      sadd,
	"lpush":     lpush,
	"zadd":      zadd,
	"hget":      hget,
	"hdel":      hdel,
	"sismenber": sismenber,
	"srem":      srem,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zscore":    zscore,
}

type BitcaskClient struct {
	// 获取Server端
	server *BitcaskServer
	// 获取当前数据库的示例
	db *bitcask_redis.RedisDataStructureType
}

// execClientCommand 执行客户端传递过来的命令
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	// 获取用户传入指令的第一个参数(set / get / ...)
	command := strings.ToLower(string(cmd.Args[0])) //转化为小写

	// 将存放于上下文中的客户端信息拿出来
	client, _ := conn.Context().(*BitcaskClient)

	//处理不同的命令
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		// 查询我们数据库是否支持这种操作命令
		cmdFunc, ok := supportedCommands[command]
		if !ok {
			conn.WriteError("Err unsupposed command: ' " + command + " ' ")
			return
		}

		//第一个字符串已经处理了，直接从1开始索引
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				// 没找到key就写入空
				conn.WriteNull()
			} else {
				//内部错误
				conn.WriteError(err.Error())
			}
			return
		}
		// 写入数据
		conn.WriteAny(res)
	}
}

/////////// String

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	// 如果用户指令不符合要求，直接返回错误
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SET")
	}

	// set key value ,我们只需要处理 key 和 value 因为操作类型已经确定
	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}

	//设置成功
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	// 如果用户指令不符合要求，直接返回错误
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("GET")
	}

	key := args[0]

	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}

	return value, nil
}

////////// Hash 表

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	// 如果用户指令不符合要求，直接返回错误
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("HSET")
	}

	var ok = 0 //返回值

	key, field, value := args[0], args[1], args[2]

	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}

	//如果成功
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("HGET")
	}

	key, field := args[0], args[1]

	value, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func hdel(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("HDEL")
	}

	var ok = 0
	key, field := args[0], args[1]

	res, err := cli.db.HDelete(key, field)
	if err != nil {
		return nil, err
	}

	if res {
		ok = 1
	}

	return ok, nil
}

//////////////// Set

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	// key 和 member
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SADD")
	}

	key, member := args[0], args[1]

	var ok = 0
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}

	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func sismenber(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SISMEMBER")
	}

	key, member := args[0], args[1]

	exist, err := cli.db.SIsMenber(key, member)
	if err != nil {
		return nil, err
	}

	return exist, nil
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("SREM")
	}

	var ok = 0
	key, member := args[0], args[1]

	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}

	if res {
		ok = 1
	}

	return ok, nil
}

/////////////// List

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("LPUSH")
	}

	key, value := args[0], args[1]

	currsize, err := cli.db.LPush(key, value)
	if err != nil {
		return nil, err
	}

	// 返回列表当前有多少数据
	return redcon.SimpleInt(currsize), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("RPUSH")
	}

	key, value := args[0], args[1]

	currsize, err := cli.db.RPush(key, value)
	if err != nil {
		return nil, err
	}

	// 返回列表当前有多少数据
	return redcon.SimpleInt(currsize), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("LPOP")
	}

	key := args[0]

	value, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}

	// 返回左边的第一个值
	return value, nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("RPOP")
	}

	key := args[0]

	value, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}

	// 返回左边的第一个值
	return value, nil
}

// /////////////// ZSET

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("ZADD")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]

	res, err := cli.db.ZADD(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}

	//如果添加成功
	if res {
		ok = 1
	}

	// 返回是否添加成功
	return redcon.SimpleInt(ok), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("ZSCORE")
	}

	key, member := args[0], args[1]

	score, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleString(score), nil
}
