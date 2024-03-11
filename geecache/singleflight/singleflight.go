package singleflight

import "sync"

/*
缓存穿透: 缓存穿透是指缓存和数据库中都没有的数据，而用户不断发起请求，如发起对不存在的ID进行查询，由于缓存不命中会直接查数据库，如果数据库中也没有，那么每次针对这个ID的请求都会穿透缓存直接查数据库，如果数据库压力过大有可能会导致宕机。这种情况也叫缓存穿透。
缓存击穿: 缓存击穿是指一个key非常热点，在不停的扛着大并发，大并发集中对这一个点进行访问，当这个Key在失效的瞬间，持续的大并发就穿破缓存，直接请求数据库，就像在一个屏障上凿开了一个洞。
缓存雪崩: 缓存雪崩是指在设置缓存时采用了相同的过期时间，导致缓存在某一时刻同时失效，请求全部转发到DB，DB瞬时压力过重雪崩。
*/

// 表示正在进行当中，或者已经结束的请求
type call struct {
	wg  sync.WaitGroup // 锁避免重入
	val interface{}
	err error
}

// 管理不同 key 的请求(call)
type Group struct {
	mu sync.Mutex       // 保护 Group 的成员变量 m 不被并发读写而加上的锁
	m  map[string]*call //
}

// 无论 Do 被调用多少次，函数 fn 都只会被调用一次，等待 fn 调用结束了，返回返回值和错误
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()         // 如果请求正在进行中，则等待
		return c.val, c.err // 请求结束，返回结果
	}

	c := new(call)
	c.wg.Add(1)  // 发送请求前加锁
	g.m[key] = c // 添加到 g.m, 表明 key 已经有对应的请求在处理
	g.mu.Unlock()

	c.val, c.err = fn() // 调用 fn, 发送请求
	c.wg.Done()         // 请求结束

	g.mu.Lock()
	delete(g.m, key) // 更新 g.m
	g.mu.Unlock()

	return c.val, c.err // 返回结果
}
