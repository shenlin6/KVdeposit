package redis

import (
	"encoding/binary"
	"errors"
	"time"

	"bitcask.go"
	"bitcask.go/utils"
)

type RedisDataType = byte

// 设置redis数据结构类型常量
const (
	String RedisDataType = iota
	Hash
	Set
	List
	ZSet
)

// 错误类型
var ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// RedisDataType 处理数据结构
type RedisDataStructureType struct {
	db *bitcask.DB
}

// NewRedisDataType 初始化处理数据结构的服务
func NewRedisDataStructureType(options bitcask.Options) (*RedisDataStructureType, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructureType{
		db: db,
	}, nil
}


// 关闭数据库
func(r *RedisDataStructureType)Close()error{
	return r.db.Close()
}



/////// string

// Set
func (r *RedisDataStructureType) Set(key []byte, ttl time.Duration, value []byte) error {
	//如果用户没有传入 value 不继续执行逻辑
	if value == nil {
		return nil
	}

	// 编码方式： value = type +expireTime+payload(原始的value)

	buf := make([]byte, binary.MaxVarintLen64+1) //存储编码后的数据(+1保证可以容纳最大长度的变长整数编码)
	buf[0] = String                              // 存数据类型
	var index = 1
	var expireTime int64 = 0
	if ttl != 0 {
		//如果用户传递的时间不为零的话，设置过期时间(用纳秒表示)
		expireTime = time.Now().Add(ttl).UnixNano()
	}
	//编码过期时间，并且移动索引位置
	index += binary.PutVarint(buf[index:], expireTime)

	//开始编码 value
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	//编码后写入
	return r.db.Put(key, encValue)
}

// Get
func (r *RedisDataStructureType) Get(key []byte) ([]byte, error) {
	encvalue, err := r.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encvalue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	//拿到过期时间
	expireTime, n := binary.Varint(encvalue[index:])
	index += n

	//如果过期了就直接返回
	if expireTime > 0 && expireTime <= time.Now().UnixNano() {
		return nil, nil
	}

	// 拿到原始数据并返回
	return encvalue[index:], nil
}

/////// Hash 表

func (r *RedisDataStructureType) HSet(key, field, value []byte) (bool, error) { //bool 表示操作是否成功（及时field存在也返回false）
	//查找对应的元数据
	metadata, err := r.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Key
	hk := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}

	encKey := hk.encode()

	// 找到 Key 对应的数据是否存在
	var exist = true
	if _, err := r.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	//保证数据更新的原子性
	wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	// 不存在我们需要更新元数据
	if !exist {
		metadata.size++
		_ = wb.Put(key, metadata.encodeMetaData())
	}

	// 存在的话我们也需要更新数据
	_ = wb.Put(encKey, value)

	//提交WriteBatch
	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (r *RedisDataStructureType) HGet(key, field []byte) ([]byte, error) {
	// 查找元数据
	metadata, err := r.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	// 如果元数据中 size 为0 说明根本没有 value
	if metadata.size == 0 {
		return nil, nil
	}

	// 构造 Key
	hk := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}

	// 获取存储的数据
	return r.db.Get(hk.encode())
}

// HDelete 根据 key 和 feild 删除数据
func (r *RedisDataStructureType) HDelete(key, field []byte) (bool, error) {
	// 查找元数据
	metadata, err := r.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 如果元数据中 size 为0 说明根本没有 value
	if metadata.size == 0 {
		return false, nil
	}

	// 构造 Key
	hk := &hashInternalKey{
		key:     key,
		version: metadata.version,
		field:   field,
	}

	encKey := hk.encode()

	//查看编码后的 Key 是否存在
	var exist = true
	if _, err := r.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// 存在的话才删除，并且递减 size
	if exist {
		wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		metadata.size--

		//将修改后的元数据重新 Put 一遍
		_ = wb.Put(key, metadata.encodeMetaData())

		//删除数据
		_ = wb.Delete(encKey)

		// 提交事务批次
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}

	// 返回是否删除成功
	return exist, nil
}

//////////////// Set

func (r *RedisDataStructureType) SAdd(key, member []byte) (bool, error) {

	// 查找对应元数据
	metadata, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 构造数据部分的 Key
	sk := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	var ok bool

	// 看这个Key是否存在，存在就跳过，不存在就添加
	if _, err := r.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		// 不存在就更新(WriteBatch 保证原子性)
		wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		metadata.size++
		// 把元数据放进存储器
		_ = wb.Put(key, metadata.encodeMetaData())

		//数据部分
		_ = wb.Put(sk.encode(), nil) //数据部分对应的value为空，因为没用

		//提交事务
		if err := wb.Commit(); err != nil {
			return false, err
		}
		ok = true
	}

	return ok, nil
}

// SIsMenber 某个member 是否属于 某个 key
func (r *RedisDataStructureType) SIsMenber(key, member []byte) (bool, error) {
	// 查找对应元数据
	metadata, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 元数据不存在直接返回
	if metadata.size == 0 {
		return false, nil
	}

	// 构造数据部分的 Key
	sk := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	// 去引擎内部查找
	_, err = r.db.Get(sk.encode())
	if err != nil && err != bitcask.ErrKeyNotFound {
		// 内部错误
		return false, err
	}

	// 不存在返回false
	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	// 存在返回true
	return true, nil
}

func (r *RedisDataStructureType) SRem(key, member []byte) (bool, error) {
	// 查找对应元数据
	metadata, err := r.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	// 元数据不存在直接返回
	if metadata.size == 0 {
		return false, nil
	}

	// 构造数据部分的 Key
	sk := &setInternalKey{
		key:     key,
		version: metadata.version,
		member:  member,
	}

	if _, err = r.db.Get(sk.encode()); err == bitcask.ErrKeyNotFound {
		//不存在直接返回
		return false, nil
	}

	// 更新元数据
	wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	metadata.size--

	// 将元数据部分更新
	_ = wb.Put(key, metadata.encodeMetaData())

	// 实际删除原来的Key
	_ = wb.Delete(sk.encode())

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

/////////////// List

func (r *RedisDataStructureType) LPush(key, element []byte) (uint32, error) {
	return r.pushInner(key, element, true)
}

func (r *RedisDataStructureType) RPush(key, element []byte) (uint32, error) {
	return r.pushInner(key, element, false)
}

func (r *RedisDataStructureType) LPop(key []byte) ([]byte, error) {
	return r.PopInner(key, true)
}

func (r *RedisDataStructureType) RPop(key []byte) ([]byte, error) {
	return r.PopInner(key, false)
}

// pushInner L/R Push 通用的
func (r *RedisDataStructureType) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	//查找元数据
	data, err := r.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	// / 构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: data.version,
	}

	// 判断从左边还是右边进行操作
	if isLeft {
		lk.index = data.head - 1
	} else {
		lk.index = data.tail
	}

	//更新元数据和数据
	wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	//新加入了一个元素，+1
	data.size++
	if isLeft {
		data.head--
	} else {
		data.tail++
	}

	//存储实际的数据
	_ = wb.Put(key, data.encodeMetaData())
	_ = wb.Put(lk.encode(), element)

	//提交事务
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return data.size, nil
}

// // PopInner L/R Pop 通用的
func (r *RedisDataStructureType) PopInner(key []byte, isLeft bool) ([]byte, error) {
	// 查找元数据
	data, err := r.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	// 如果没有Key，直接返回
	if data.size == 0 {
		return nil, nil
	}

	// / 构造数据部分的key
	lk := &listInternalKey{
		key:     key,
		version: data.version,
	}

	// 判断从左边还是右边进行操作
	if isLeft {
		lk.index = data.head
	} else {
		lk.index = data.tail - 1
	}

	element, err := r.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	data.size--
	if isLeft {
		data.head++
	} else {
		data.tail--
	}
	if err := r.db.Put(key, data.encodeMetaData()); err != nil {
		return nil, err
	}

	return element, nil
}

// /////////////// ZSET

func (r *RedisDataStructureType) ZADD(key []byte, score float64, member []byte) (bool, error) {
	data, err := r.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	// 构造数据部分的 Key
	z := &zsetInternalKey{
		key:     key,
		member:  member,
		version: data.version,
		score:   score,
	}

	// 查看这个 Key 是否已经存在
	var exist = true
	// 拿到 score 的值
	scoreValue, err := r.db.Get(z.encodeMember())
	if err != nil && err != bitcask.ErrKeyNotFound {
		// 程序的内部错误
		return false, err
	}
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}
	if exist {
		// 判断 score 是否一样，是则返回错误，不是则存储
		if score == utils.FloatFromBytes(scoreValue) {
			return false, nil
		}
	}

	// 更新元数据
	wb := r.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		// 如果不存在则存储
		data.size++
		_ = wb.Put(key, data.encodeMetaData())
	}
	// 如果存在的话将旧的Key对应的数据删除,否则迭代遍历的时候旧的Key也会被查询出来
	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			member:  member,
			version: data.version,
			score:   utils.FloatFromBytes(scoreValue),
		}
		// 删除旧的 value 值
		_ = wb.Delete(oldKey.encodeMember())
	}

	// 更新数据
	_ = wb.Put(z.encodeMember(), utils.Float64ToBytes(score))
	_ = wb.Put(z.encodeScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}

	// 返回 ！exist ：方便调用者知道是否成功添加了新的成员
	return !exist, nil
}

func (r *RedisDataStructureType) ZScore(key []byte, member []byte) (string, error) {
	// 注意:! ! ! score 是负数暂时不支持这种存储
	data, err := r.findMetadata(key, ZSet)
	if err != nil {
		return "", err // -1 表示操作失败
	}

	if data.size == 0 {
		return "", nil
	}

	// 构造数据部分的 Key
	z := &zsetInternalKey{
		key:     key,
		member:  member,
		version: data.version,
	}

	scoreValue, err := r.db.Get(z.encodeMember())
	if err != nil {
		return "", err
	}

	// 返回对应的 score
	return utils.BytesToString(scoreValue), nil
}

// findMetadata 查找元数据
func (r *RedisDataStructureType) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	//获取元数据
	metaBuf, err := r.db.Get(key)

	if err != nil && err != bitcask.ErrKeyNotFound {
		// 如果没找到说明可能需要创建一个新的Key，不代表错误
		return nil, err
	}

	var data *metadata
	// 标记元数据是否存在
	var metaDataExist = true

	// 没找到或者过期，需要初始化元数据
	if err == bitcask.ErrKeyNotFound {
		metaDataExist = false
	} else {
		//存在的话就解码
		data = decodeMetaData(metaBuf)

		if data.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}

		//如果过期了，也标记不存在
		if data.expireTime != 0 && data.expireTime <= time.Now().UnixNano() {
			metaDataExist = false
		}
	}

	// 不存在则初始化一个
	if !metaDataExist {
		data = &metadata{
			dataType:   dataType,
			expireTime: 0,
			version:    time.Now().UnixNano(),
			size:       0,
		}

		// 对 List 类型特殊处理
		if data.dataType == List {
			data.head = initialListMark
			data.tail = initialListMark
		}
	}

	//初始化好后直接返回元数据
	return data, nil
}
