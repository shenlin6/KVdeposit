package redis

// 通用的操作

// Delete 删除
func (r *RedisDataStructureType) Delete(key []byte) error {
	return r.db.Delete(key)
}

// Type 获取一个 Key 的类型
func (r *RedisDataStructureType) Type(key []byte) (RedisDataType, error) {
	// 直接获取第一个字节即可
	encvalue, err := r.db.Get(key)
	if err != nil {
		return 0, err
	}

	// 如果本来就没存Type，直接返回，防止数组越界
	if len(encvalue) == 0 {
		return 0, nil
	}

	// 返回
	return encvalue[0], nil
}
