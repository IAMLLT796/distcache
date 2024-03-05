package geecache

/*
	ByteView用来表示缓存值
*/
// ByteView 保存了一个不可变的字节视图
type ByteView struct {
	// b 是只读的，使用 ByteSlice() 方法返回一个拷贝，防止缓存值被外部程序修改
	b []byte // byte类型可以支持任意数据类型的存储
}

// 视图长度
func (c ByteView) Len() int {
	return len(c.b)
}

// byteslice 返回数据的副本作为字节片
func (c ByteView) ByteSlice() []byte {
	return cloneBytes(c.b)
}

// string 以字符串形式返回数据，必要时生成副本
func (c ByteView) String() string {
	return string(c.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
