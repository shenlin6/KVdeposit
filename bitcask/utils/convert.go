package utils

import "strconv"

// 将浮点数类型转化为字符数组类型

// FloatFromBytes 将字节切片转换为对应的浮点数值
func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

// Float64ToBytes 将浮点数类型转化为字符切片类型
func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}

// 将字节切片解释为字符串
func BytesToString(byteSlice []byte) string {
    return string(byteSlice)
}