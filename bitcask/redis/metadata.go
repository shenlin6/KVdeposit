package redis

import (
	"encoding/binary"
	"math"

	"bitcask.go/utils"
)

const (
	// 元数据占用的字节数量
	maxMetadataSize = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32 //(1+5*4+5)

	// 因为 List 有两部分数据
	ListMetadataSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

// metadata 元数据
type metadata struct {
	dataType   byte   //数据类型
	expireTime int64  //过期时间
	version    int64  //版本号
	size       uint32 //数据大小
	head       uint64 //List 头部索引
	tail       uint64 //List 尾部索引
}

// encodeMetaData 编码元数据，转化为字节数组类型
func (md *metadata) encodeMetaData() []byte {
	var size = maxMetadataSize

	// 如果发现是 List 类型，多加上 List 专属的字段
	if md.dataType == List {
		size += ListMetadataSize
	}

	buf := make([]byte, size)

	//获取数据类型
	buf[0] = md.dataType
	var index = 1

	//获取过期时间、版本号和数据Size，并调整索引
	index += binary.PutVarint(buf[index:], md.expireTime)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	//如果是 List 类型还要取出 head 和 tail
	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	//返回编码后数据
	return buf[:index]
}

// decodeMetaData 解码元数据
func decodeMetaData(buf []byte) *metadata {
	// 获取数据类型
	dataType := buf[0]
	var index = 1

	//获取过期时间
	expireTime, n := binary.Varint(buf[index:])
	index += n
	//版本号
	version, n := binary.Varint(buf[index:])
	index += n
	//Size
	size, n := binary.Varint(buf[index:])
	index += n

	//对 List 类型进行特殊处理
	var head uint64 = 0
	var tail uint64 = 0

	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}

	//返回解码后的元数据
	return &metadata{
		dataType:   dataType,
		expireTime: expireTime,
		version:    version,
		size:       uint32(size),
		head:       head,
		tail:       tail,
	}
}

// hashInternalKey Hash类型需要编码成真正Key的结构体
type hashInternalKey struct {
	key     []byte
	field   []byte
	version int64
}

// 编码 Hash 真正的Key
func (h *hashInternalKey) encode() []byte {
	// 计算字节数
	buf := make([]byte, len(h.key)+len(h.field)+8) //version最多占八位

	// key
	var index = 0
	copy(buf[index:index+len(h.key)], h.key)
	index += len(h.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(h.version)) //确定字节大小的类型就用小端序
	index += 8                                                           //最多 8 字节

	// field
	copy(buf[index:], h.field)

	return buf
}

type setInternalKey struct {
	key     []byte
	member  []byte
	version int64
}

// 编码 Set
func (s *setInternalKey) encode() []byte {
	// 计算字节数
	buf := make([]byte, len(s.key)+len(s.member)+8+4) // 还要加上 member的 size :4个字节

	// key
	var index = 0
	copy(buf[index:index+len(s.key)], s.key)
	index += len(s.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(s.version))
	index += 8 //最多 8 字节

	// member
	copy(buf[index:index+len(s.member)], s.member)
	index += len(s.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(s.member)))

	return buf
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (l *listInternalKey) encode() []byte {
	buf := make([]byte, len(l.key)+8*2)

	// 编码 key
	var index = 0
	copy(buf[index:index+len(l.key)], l.key)
	index += len(l.key)

	// 编码 version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(l.version))
	index += 8

	// 编码 index
	binary.LittleEndian.PutUint64(buf[index:index+8], l.index)

	return buf
}

type zsetInternalKey struct {
	key     []byte
	member  []byte
	version int64
	score   float64
}

// 编码 member 部分
func (z *zsetInternalKey) encodeMember() []byte {
	buf := make([]byte, len(z.key)+len(z.member)+8)

	// 编码 key
	var index = 0
	copy(buf[index:index+len(z.key)], z.key)
	index += len(z.key)

	// 编码 version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(z.version))
	index += 8

	//编码 member
	copy(buf[index:index+len(z.member)], z.member)

	return buf
}

// 编码 score 部分
func (z *zsetInternalKey) encodeScore() []byte {
	scoreBuf := utils.Float64ToBytes(z.score)

	buf := make([]byte, len(z.key)+len(z.member)+len(scoreBuf)+8+4)

	// 编码 key
	var index = 0
	copy(buf[index:index+len(z.key)], z.key)
	index += len(z.key)

	// 编码 version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(z.version))
	index += 8

	// 编码 score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	//编码 member
	copy(buf[index:index+len(z.member)], z.member)
	index += len(z.member)

	// 加上 member 的长度 member 的 size ,方便我们获取 member
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(z.member)))

	return buf
}
