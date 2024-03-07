package geecache

import (
	"fmt"
	"log"
	"sync"
)

// Getter loads data for a key
type Getter interface {
	Get(key string) ([]byte, error) // 回调函数
}

// GetterFunc implements Getter with a function
type GetterFunc func(key string) ([]byte, error)

// Get implements Getter interface function
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 一个 Group 可以认为是一个缓存命名空间，每个 Group 拥有一个唯一的名称 name
type Group struct {
	name      string // 每个 Group 拥有一个唯一的名称 name
	getter    Getter // 缓存未命中时获取数据的回调(callback)
	mainCache cache  // 一开始实现的并发缓存
	peers     PeerPicker
}

// 注册节点选择器
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

func (g *Group) load(key string) (value ByteView, err error) {
	if g.peers != nil {
		if peer, ok := g.peers.PickPeer(key); ok {
			if value, err = g.getFromPeer(peer, key); err != nil {
				return value, err
			}
			log.Println("[distcache] Failed to get from peer", err)
		}
	}
	return g.getLocally(key)
}

func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	bytes, err := peer.Get(g.name, key) // 从对应 group 查找缓存值
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: cloneBytes(bytes)}, nil

}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// 创建一个 Group 实例
func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
	if getter == nil {
		panic("nil Getter")
	}
	mu.Lock()
	defer mu.Unlock()

	g := &Group{
		name:      name,
		getter:    getter,
		mainCache: cache{cacheBytes: cacheBytes},
	}
	groups[name] = g

	return g
}

// 获取指定名称的 Group
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[DistCache] hit")
		return v, nil
	}
	return g.load(key)
}

func (g *Group) getLocally(key string) (ByteView, error) {
	bytes, err := g.getter.Get(key)
	if err != nil {
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}
func (g *Group) populateCache(key string, value ByteView) {
	g.mainCache.add(key, value)
}
