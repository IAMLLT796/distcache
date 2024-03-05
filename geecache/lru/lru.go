package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes int64                    // 允许使用的最大内存
	nbytes   int64                    // 当前已经使用的内存
	ll       *list.List               // 双向链表
	cache    map[string]*list.Element // 字典，key 是字符串，value 是双向链表中对应节点的指针
	// optional and executed when an entry is purged
	OnEvicted func(key string, value Value) // 某条记录被移除时的回调函数，可以为 nil
}

/*
双向链表节点的数据类型
*/
type entry struct {
	key   string
	value Value
}

/*
允许值是实现了 Value 接口的任意类型，该接口只包含了一个方法，用于返回值所占用的内存大小
*/
type Value interface {
	Len() int
}

// 为了实例化 Cache，实现 New() 函数
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

/*
实现查找功能：

		1.从字典中找到对应的双向链表的节点；
	    2.将该节点移动到队尾；
*/
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)

		return kv.value, true
	}
	return
}

/*
实现删除功能：缓存淘汰，移除最近最少访问的节点（队首）
*/
func (c *Cache) RemoveOldest() {
	ele := c.ll.Back() // 取到队首节点，从链表中删除
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry) // 尝试将双向链表元素中存储的数据转换为一个缓存条目
		delete(c.cache, kv.key)  // 从字典中删除该节点的映射关系
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())

		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

/*
新增/修改功能
*/
func (c *Cache) Add(key string, value Value) {
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele) // 如果键存在，则更新对应节点的值，并将该节点移到队尾
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest() // 如果添加新数据后总内存超过了设定的最大值，则移除最少访问的节点
	}
}

// 获取添加了多少条数据
func (c *Cache) Len() int {
	return c.ll.Len()
}
