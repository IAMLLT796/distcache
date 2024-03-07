package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

/*
一致性 Hash 算法 解决的问题:
	1. 访问稳定的缓存节点
	2. 解决缓冲节点数量变化问题，比如缓存雪崩，缓存在同一时刻全部失效，造成瞬时 DB 请求量大，压力骤增，引起雪崩
       这是由于缓存服务器宕机，或者缓存了相同的过期时间引起的
	3. 在新增或者/删除节点时，只需要重新定位该节点附近的一小部分数据，而不需要重新定位所有的节点
*/

// Hash maps bytes to uint32
// Hash 函数类型，采用依赖注入的方式，允许用于替换成自定义的 Hash 函数，也方便测试替换，默认为 crc32.ChecksumIEEE 算法
type Hash func(data []byte) uint32

// Map 为一致性哈希算法的主数据结构
type Map struct {
	hash     Hash
	replicas int            // 虚拟节点倍数
	keys     []int          // Sorted	// 保存所有的虚拟节点的哈希值，哈希环
	hashMap  map[int]string // 虚拟节点与真实节点的映射表，键是虚拟节点的哈希值，值是真实节点的名称
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// 添加真实节点/机器的 Add() 方法
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) // 为每个真实节点 key 生成 m.replicas 个虚拟节点
			m.keys = append(m.keys, hash)                      // 将虚拟节点的哈希值添加到环上
			m.hashMap[hash] = key                              // 在 hashMap 中增加虚拟节点和真实节点的映射关系
		}
	}
	sort.Ints(m.keys)
}

// Get 方法根据输入的 key，返回最靠近它的那个节点的信息
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))

	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
